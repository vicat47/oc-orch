"""提供商故障切换台账：meta 自有命名空间 + 短事务 + CAS（provider-fault-failover）。

只复用 ``meta`` 表与 :func:`oc_orch.adapters.sqlite_db.transaction`，**不建新业务
表、不改公共 schema**。命名空间：

- ``failover:schema`` — 本存储 schema 版本；
- ``failover:episode:{episode_id}`` — 一次故障链事实（原始失败路由/预算/状态）；
- ``failover:operation:{operation_id}`` — 一次恢复操作（切换/接续意图，含可能提交）；
- ``failover:policy:{scope}:{scope_id}`` — 版本化故障切换策略（项目默认/任务覆盖）；
- ``failover:route-block:{route}`` — 原路线硬故障阻断（备用成功/时间流逝不解锁）。

安全不变量（specs/provider-fault-failover F15/F25/F26 的存储面）：

- **fail-closed 校验**：``_load``/``_store`` 统一过 ``_validate_record``；坏字段、
  非当前 schema、revision 非整数一律抛 :class:`FailoverStateError`，不静默规整。
- **短事务 + CAS**：每次写入在 ``BEGIN IMMEDIATE`` 内重读并校验期望版本；并发
  双写只能一个成功，另一个以 ``stale`` 拒绝。网络永不进入本层。
- **可能提交**：``operation`` 在切换/接续网络请求**之前**持久化
  ``possible_submitted_at``；崩溃后该行保持“已可能提交”，不因重启无条件重发。
- **混合版本拒写**：本存储记录自带 ``schema`` 字段；写入时若台账 meta schema
  与既有 failover 记录冲突则拒绝（能力写契约由任务5.1统一安装，本层提供
  ``guard_valid`` 只读投影与坏记录可见错误）。
- 只读投影不推进任何游标；恢复调谐使用的公平游标另存
  ``failover:cursor:{kind}``。
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable

from ..util import json_dumps, json_loads, short_id
from ..domain.errors import FailoverStateError
from .sqlite_db import meta_get, meta_set, transaction
from .failover_guard import check_failover_guard, session_lock_key

FAILOVER_SCHEMA = 1
FAILOVER_SCHEMA_KEY = "failover:schema"
EPISODE_PREFIX = "failover:episode:"
OPERATION_PREFIX = "failover:operation:"
POLICY_PREFIX = "failover:policy:"
ROUTE_BLOCK_PREFIX = "failover:route-block:"
CURSOR_PREFIX = "failover:cursor:"

MAX_CLOCK_MS = 8 * 10 ** 12
MAX_RECORD_BYTES = 64 * 1024

# 记录种类 → 必需键（额外键原样保留；未知/多余字段不拒绝，便于向前兼容）。
_RECORD_KINDS: dict[str, tuple[str, ...]] = {
    "episode": ("kind", "episode_id", "project", "task_id", "session_id",
                "route", "category", "status", "revision", "switches_used",
                "attempts_used", "created_at", "updated_at"),
    "operation": ("kind", "operation_id", "episode_id", "session_id",
                  "status", "revision", "possible_submitted_at",
                  "created_at", "updated_at"),
}


from ..domain.failover import episode_id as failover_episode_id  # noqa: E402


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
    if not _is_int_not_bool(limit) or not 1 <= limit <= 200:
        raise ValueError("limit 必须为 1..200 的整数")
    return int(limit)


def _strict_after(value: Any) -> str | None:
    if value is None:
        return None
    if not _nk(value):
        raise ValueError("after_id 必须是非空字符串或 None")
    return value


def _record_key(kind: str, record_id: str) -> str:
    prefix = {"episode": EPISODE_PREFIX, "operation": OPERATION_PREFIX}[kind]
    return f"{prefix}{record_id}"


# ---- 记录校验 ----------------------------------------------------------------
def _validate_record(rec: dict) -> None:
    """严格结构校验（fail-closed）。"""
    kind = rec.get("kind")

    def bad(msg: str) -> None:
        raise FailoverStateError(f"failover 记录非法（{rec.get('kind')}/{rec.get('episode_id') or rec.get('operation_id') or '?'}）：{msg}")

    if rec.get("schema") != FAILOVER_SCHEMA:
        bad(f"schema={rec.get('schema')!r}")
    if kind not in _RECORD_KINDS:
        bad(f"kind={kind!r}")
    record_id = rec.get("episode_id") if kind == "episode" else rec.get("operation_id")
    if not _nk(record_id):
        bad("记录 id 缺失")
    if not isinstance(rec.get("revision"), int) or isinstance(rec.get("revision"), bool) \
            or rec["revision"] < 1:
        bad("revision 必须为正整数")
    for key in _RECORD_KINDS[kind]:
        if key in ("revision", "switches_used", "attempts_used"):
            if not _is_int_not_bool(rec.get(key)) or rec[key] < 0:
                bad(f"{key} 必须为非负整数")
            continue
        if key in ("created_at", "updated_at"):
            if _valid_ms(rec.get(key)) is None:
                bad(f"{key} 非法")
            continue
        if key == "possible_submitted_at":
            if _valid_ms(rec.get(key)) is None and rec.get(key) is not None:
                bad("possible_submitted_at 非法")
            continue
        if key == "session_id" and rec.get(key) is None:
            continue  # 尚未归属 session 的 episode 允许为空
        if not _nk(rec.get(key)):
            bad(f"缺少非空字段 {key}")
    for key in ("created_at", "updated_at"):
        if _valid_ms(rec.get(key)) is None:
            bad(f"{key} 非法")
    if "updated_at" in rec and _valid_ms(rec.get("created_at")) is not None \
            and _valid_ms(rec.get("updated_at")) < _valid_ms(rec.get("created_at")):
        bad("updated_at < created_at")


# ---- 策略校验 -----------------------------------------------------------------
def _validate_policy(policy: dict) -> None:
    def bad(msg: str) -> None:
        raise FailoverStateError(f"failover 策略非法：{msg}")

    for key in ("scope", "scope_id", "enabled", "revision", "created_at",
                "updated_at"):
        if key not in policy:
            bad(f"缺少字段 {key}")
    if not (_nk(policy.get("scope")) and _nk(policy.get("scope_id"))):
        bad("scope/scope_id 必须为非空字符串")
    if not isinstance(policy.get("enabled"), bool):
        bad("enabled 必须为 bool")
    if not _is_int_not_bool(policy.get("revision")) or policy["revision"] < 1:
        bad("revision 必须为正整数")
    if _valid_ms(policy.get("created_at")) is None or \
            _valid_ms(policy.get("updated_at")) is None:
        bad("时间字段非法")
    source_routes = policy.get("source_routes")
    if source_routes is not None and not isinstance(source_routes, list):
        bad("source_routes 必须为列表或 None")
    fallback = policy.get("fallback_routes")
    if fallback is not None and not isinstance(fallback, list):
        bad("fallback_routes 必须为列表或 None")
    for key in ("max_switches", "max_attempts", "notify_updates"):
        value = policy.get(key)
        if value is not None and (not _is_int_not_bool(value) or value < 0):
            bad(f"{key} 必须为非负整数")


class FailoverLedger:
    """provider 故障切换台账（meta 自有命名空间）。"""

    def __init__(self, conn: sqlite3.Connection, *, clock: Callable[[], int],
                 audit: Callable | None = None):
        self.conn = conn
        self._now = clock
        self._audit = audit

    def _now_ms(self) -> int:
        return int(self._now())

    def _audit_log(self, action: str, entity: str, entity_id: str,
                   detail: dict | None = None) -> None:
        if self._audit is not None:
            try:
                self._audit("supervisor", action, entity, entity_id, detail)
            except Exception:
                pass  # 审计失败不阻断业务写入（任务5.1写契约单独校验）

    def _read_schema(self) -> bool:
        """只读检查功能 schema；查询不能隐式写 marker（F25）。"""
        version = meta_get(self.conn, FAILOVER_SCHEMA_KEY)
        if version is None:
            return False
        if str(version) != str(FAILOVER_SCHEMA):
            raise FailoverStateError(f"failover schema 不兼容：{version!r}")
        return True

    def _require_guard(self) -> None:
        try:
            valid = check_failover_guard(self.conn)
        except Exception as exc:
            raise FailoverStateError(f"failover 写契约无效：{exc}") from exc
        if not valid:
            raise FailoverStateError("failover 写契约未显式安装；自动恢复保持关闭")

    def _ensure_schema(self) -> None:
        version = meta_get(self.conn, FAILOVER_SCHEMA_KEY)
        if version is None:
            meta_set(self.conn, FAILOVER_SCHEMA_KEY, str(FAILOVER_SCHEMA))
        elif str(version) != str(FAILOVER_SCHEMA):
            raise FailoverStateError(f"failover schema 不兼容：{version!r}")

    def _load(self, kind: str, record_id: str) -> dict | None:
        raw = meta_get(self.conn, _record_key(kind, record_id))
        if raw is None:
            return None
        rec = json_loads(raw, default=None)
        if not isinstance(rec, dict):
            raise FailoverStateError(
                f"failover 记录损坏（{kind}/{record_id}）：非对象")
        _validate_record(rec)
        return rec

    def _store(self, record_id: str, rec: dict) -> None:
        payload = json_dumps(rec).encode("utf-8")
        if len(payload) > MAX_RECORD_BYTES:
            raise FailoverStateError(
                f"failover 记录过大（{record_id}）：{len(payload)} 字节")
        _validate_record(rec)
        meta_set(self.conn, _record_key(rec["kind"], record_id), json_dumps(rec))

    # ---- episode / operation 通用 upsert（CAS 短事务） ---------------------
    def upsert_record(self, kind: str, record_id: str, record: dict, *,
                      expected_revision: int | None) -> dict:
        """创建或按期望版本更新。expected_revision=None 表示仅创建（已存在拒绝）。"""
        if kind not in _RECORD_KINDS:
            raise FailoverStateError(f"未知记录种类：{kind!r}")
        if not _nk(record_id):
            raise ValueError("record_id 必须为非空字符串")
        now = self._now_ms()
        base = {
            "schema": FAILOVER_SCHEMA,
            "kind": kind,
            "revision": 0,
            "created_at": now,
            "updated_at": now,
            **record,
        }
        with transaction(self.conn):
            self._require_guard()
            self._ensure_schema()
            existing = self._load(kind, record_id)
            if existing is None:
                if expected_revision is not None:
                    raise FailoverStateError(
                        f"failover CAS 冲突：记录不存在但期望版本 {expected_revision}")
                rec = {**base, "revision": 1}
                created = True
            else:
                if expected_revision is None:
                    raise FailoverStateError(
                        f"failover CAS 冲突：记录已存在（revision="
                        f"{existing.get('revision')}）且未给期望版本")
                if int(existing["revision"]) != int(expected_revision):
                    raise FailoverStateError(
                        f"failover CAS 冲突：期望 {expected_revision}，"
                        f"实际 {existing['revision']}（record_id={record_id}）")
                rec = {**existing, **base,
                       "revision": int(existing["revision"]) + 1,
                       "created_at": existing["created_at"], "updated_at": now}
                created = False
            _validate_record(rec)
            self._store(record_id, rec)
        self._audit_log(f"failover.{kind}.{'create' if created else 'update'}",
                        kind, record_id,
                        {"revision": rec["revision"], "status": rec.get("status"),
                         "project": rec.get("project")})
        return {"created": created, "record": rec, "revision": rec["revision"]}

    def prepare_switch(self, *, episode_id: str, operation_id: str,
                       expected_revision: int, target: dict, owner: str,
                       now: int | None = None) -> dict:
        """原子认领 session、保存 operation 和 possible switch（F15/F26）。

        事务提交后才允许发一次 POST。并发调谐中只有一个调用能取得 session
        lock；崩溃后保留 possible 与占用事实，不会盲目重发。
        """
        if not (_nk(episode_id) and _nk(operation_id) and _nk(owner)):
            raise ValueError("episode_id/operation_id/owner 必须为非空字符串")
        now = now if isinstance(now, int) else self._now_ms()
        with transaction(self.conn):
            self._require_guard()
            self._ensure_schema()
            episode = self._load("episode", episode_id)
            if episode is None:
                return {"claimed": False, "reason": "episode-missing"}
            if int(episode.get("revision", -1)) != int(expected_revision):
                return {"claimed": False, "reason": "stale-episode",
                        "episode": episode}
            if episode.get("status") not in ("open", "waiting_quiescence"):
                return {"claimed": False, "reason": "episode-not-open",
                        "episode": episode}
            session_id = episode.get("session_id")
            if not isinstance(session_id, str) or not session_id:
                return {"claimed": False, "reason": "session-unknown",
                        "episode": episode}
            lock_key = session_lock_key(session_id)
            if meta_get(self.conn, lock_key) is not None:
                return {"claimed": False, "reason": "session-occupied",
                        "episode": episode}
            if self._load("operation", operation_id) is not None:
                return {"claimed": False, "reason": "operation-exists",
                        "episode": episode}

            operation = {
                "schema": FAILOVER_SCHEMA, "kind": "operation",
                "operation_id": operation_id, "episode_id": episode_id,
                "session_id": session_id, "status": "switch-possible",
                "revision": 1, "possible_submitted_at": now,
                "created_at": now, "updated_at": now,
                "metadata": {
                    "target": dict(target), "owner": owner,
                    "confirm_queries": 0,
                    "next_confirm_at": now,
                    "confirm_deadline_at": now + 5 * 60 * 1000,
                },
            }
            self._store(operation_id, operation)
            meta_set(self.conn, lock_key, operation_id)
            episode["status"] = "switch_requested"
            episode["switches_used"] = int(episode.get("switches_used") or 0) + 1
            episode["metadata"] = {**dict(episode.get("metadata") or {}),
                                   "operation_id": operation_id,
                                   "target": dict(target),
                                   "possible_submitted_at": now,
                                   "next_confirm_at": now,
                                   "confirm_deadline_at": now + 5 * 60 * 1000,
                                   "confirm_queries": 0}
            episode["revision"] = int(episode["revision"]) + 1
            episode["updated_at"] = now
            self._store(episode_id, episode)
        self._audit_log("failover.operation.possible", "operation", operation_id,
                        {"episode_id": episode_id, "session_id": session_id,
                         "owner": owner})
        return {"claimed": True, "episode": episode, "operation": operation}

    def record_operation_result(self, operation_id: str, *, status: str,
                                detail: dict | None = None) -> dict:
        """记录 POST 返回类别；不改变原操作身份或释放 unknown 占用。"""
        with transaction(self.conn):
            self._require_guard()
            self._ensure_schema()
            operation = self._load("operation", operation_id)
            if operation is None:
                raise FailoverStateError("failover operation 不存在")
            operation["status"] = status
            operation["metadata"] = {**dict(operation.get("metadata") or {}),
                                      **(detail or {})}
            operation["revision"] = int(operation["revision"]) + 1
            operation["updated_at"] = self._now_ms()
            self._store(operation_id, operation)
        return operation

    def prepare_continuation(self, *, episode_id: str, operation_id: str,
                             expected_revision: int, attempt_id: str,
                             message_id: str, now: int | None = None) -> dict:
        """先持久化唯一后继身份，再允许共享 dispatch 发出输入（F19/F20）。"""
        if not all(_nk(v) for v in
                   (episode_id, operation_id, attempt_id, message_id)):
            raise ValueError("continuation identities must be non-empty")
        now = now if isinstance(now, int) else self._now_ms()
        with transaction(self.conn):
            self._require_guard()
            self._ensure_schema()
            episode = self._load("episode", episode_id)
            if episode is None:
                return {"prepared": False, "reason": "episode-missing"}
            if int(episode.get("revision", -1)) != int(expected_revision):
                return {"prepared": False, "reason": "stale-episode",
                        "episode": episode}
            operation = self._load("operation", operation_id)
            if operation is None or operation.get("episode_id") != episode_id:
                return {"prepared": False, "reason": "operation-mismatch",
                        "episode": episode}
            lock = meta_get(self.conn, session_lock_key(episode.get("session_id") or ""))
            if lock != operation_id:
                return {"prepared": False, "reason": "session-lock-mismatch",
                        "episode": episode}
            metadata = dict(episode.get("metadata") or {})
            existing_attempt = metadata.get("successor_attempt_id")
            existing_message = metadata.get("successor_message_id")
            if existing_attempt or existing_message:
                if (existing_attempt, existing_message) != (attempt_id, message_id):
                    return {"prepared": False, "reason": "continuation-identity-frozen",
                            "episode": episode}
                return {"prepared": True, "created": False, "episode": episode,
                        "operation": operation}
            if episode.get("status") != "switch_confirmed":
                return {"prepared": False, "reason": "switch-not-confirmed",
                        "episode": episode}
            metadata.update({"successor_attempt_id": attempt_id,
                             "successor_message_id": message_id,
                             "possible_continuation_at": now})
            episode["metadata"] = metadata
            episode["status"] = "continuation_requested"
            episode["revision"] = int(episode["revision"]) + 1
            episode["updated_at"] = now
            operation["status"] = "continuation-possible"
            operation["metadata"] = {**dict(operation.get("metadata") or {}),
                                      "successor_attempt_id": attempt_id,
                                      "successor_message_id": message_id,
                                      "possible_continuation_at": now}
            operation["revision"] = int(operation["revision"]) + 1
            operation["updated_at"] = now
            self._store(episode_id, episode)
            self._store(operation_id, operation)
        self._audit_log("failover.continuation.possible", "episode", episode_id,
                        {"operation_id": operation_id,
                         "successor_attempt_id": attempt_id,
                         "successor_message_id": message_id})
        return {"prepared": True, "created": True, "episode": episode,
                "operation": operation}

    def get_session_lock(self, session_id: str) -> dict | None:
        """只读读取 session 恢复占用及关联 episode/operation。"""
        if not self._read_schema():
            return None
        operation_id = meta_get(self.conn, session_lock_key(session_id))
        if not operation_id:
            return None
        operation = self._load("operation", operation_id)
        if operation is None:
            raise FailoverStateError("session lock 指向缺失 operation")
        episode = self._load("episode", operation.get("episode_id") or "")
        if episode is None:
            raise FailoverStateError("session lock 指向缺失 episode")
        return {"session_id": session_id, "operation_id": operation_id,
                "operation": operation, "episode": episode}

    def list_session_locks(self, *, limit: int = 50) -> list[dict]:
        """只读列出恢复占用；坏引用抛错，不把未知状态当作空闲。"""
        lim = _strict_limit(limit)
        if not self._read_schema():
            return []
        rows = self.conn.execute(
            "SELECT key,value FROM meta WHERE key LIKE ? ORDER BY key LIMIT ?",
            ("failover:session-lock:%", lim + 1)).fetchall()
        if len(rows) > lim:
            raise FailoverStateError("恢复占用数量超过只读投影上限")
        out = []
        for row in rows:
            session_id = str(row["key"])[len("failover:session-lock:"):]
            if not session_id:
                raise FailoverStateError("session lock 缺少 session 身份")
            lock = self.get_session_lock(session_id)
            if lock is None:
                raise FailoverStateError("session lock 读取期间消失")
            out.append(lock)
        return out

    def has_pending_input(self, session_id: str) -> bool:
        """检查会话是否有未完成的受管输入；SQL 保持在 SQLite 适配器内。"""
        row = self.conn.execute(
            "SELECT 1 FROM session_inputs WHERE session_id=? AND admission_state NOT IN "
            "('delivered','cancelled','released','recorded') LIMIT 1",
            (session_id,)).fetchone()
        return row is not None

    def list_route_blocks(self, *, limit: int = 50) -> list[dict]:
        """只读列出原路线阻断，包含已过观察窗但未人工解除的记录。"""
        lim = _strict_limit(limit)
        if not self._read_schema():
            return []
        rows = self.conn.execute(
            "SELECT key,value FROM meta WHERE key LIKE ? ORDER BY key LIMIT ?",
            (ROUTE_BLOCK_PREFIX + "%", lim + 1)).fetchall()
        if len(rows) > lim:
            raise FailoverStateError("路线阻断数量超过只读投影上限")
        out = []
        for row in rows:
            raw = json_loads(row["value"], default=None)
            if not isinstance(raw, dict) or not _nk(raw.get("route")):
                raise FailoverStateError("failover 路线阻断记录损坏")
            current = self.is_route_blocked(raw["route"])
            if current is None:
                raise FailoverStateError("failover 路线阻断读取期间消失")
            out.append(current)
        return out

    def release_session_lock(self, session_id: str, *, operation_id: str,
                             actor: str, reason: str) -> dict:
        """带明确人工理由解除恢复占用；CAS 防止误清晚到的新 operation。"""
        if not (_nk(actor) and _nk(reason) and _nk(operation_id)):
            raise ValueError("actor/reason/operation_id 必须为非空字符串")
        now = self._now_ms()
        with transaction(self.conn):
            self._require_guard()
            self._ensure_schema()
            key = session_lock_key(session_id)
            current = meta_get(self.conn, key)
            if current is None:
                return {"released": False, "reason": "not-occupied"}
            if current != operation_id:
                raise FailoverStateError("session lock operation 已变化；拒绝误清")
            operation = self._load("operation", operation_id)
            if operation is None:
                raise FailoverStateError("session lock 指向缺失 operation")
            episode_id = operation.get("episode_id")
            episode = self._load("episode", episode_id or "")
            if episode is None:
                raise FailoverStateError("session lock 指向缺失 episode")
            self.conn.execute("DELETE FROM meta WHERE key=?", (key,))
            episode["metadata"] = {**dict(episode.get("metadata") or {}),
                                   "session_lock_released_at": now,
                                   "session_lock_released_by": actor,
                                   "session_lock_release_reason": reason[:300]}
            if episode.get("status") not in ("blocked_manual", "cancelled",
                                               "superseded", "resumed"):
                episode["status"] = "blocked_manual"
                episode["metadata"]["manual_release_closed_episode"] = True
            episode["revision"] = int(episode["revision"]) + 1
            episode["updated_at"] = now
            self._store(episode_id, episode)
        self._audit_log("failover.session-lock.release", "episode", episode_id,
                        {"session_id": session_id, "operation_id": operation_id,
                         "actor": actor, "reason": reason[:200]})
        return {"released": True, "session_id": session_id,
                "operation_id": operation_id, "revision": episode["revision"]}

    def get_record(self, kind: str, record_id: str) -> dict | None:
        if not self._read_schema():
            return None
        return self._load(kind, record_id)

    def list_records(self, kind: str, *, project: str | None = None,
                     state: str | None = None, limit: int = 50,
                     after_id: str | None = None) -> dict:
        """只读 keyset 分页（不写游标；参数严格，覆盖诚实截断）。"""
        lim = _strict_limit(limit)
        after = _strict_after(after_id)
        if kind not in _RECORD_KINDS:
            raise FailoverStateError(f"未知记录种类：{kind!r}")
        if not self._read_schema():
            return {"records": [], "more": False, "next_after_id": None,
                    "invalid_records": 0}
        prefix = _record_key(kind, "")
        sql = "SELECT key, value FROM meta WHERE key LIKE ?"
        args: list[Any] = [prefix + "%"]
        if after:
            sql += " AND key > ?"
            args.append(_record_key(kind, after))
        sql += " ORDER BY key ASC LIMIT ?"
        args.append(lim + 1)
        rows = self.conn.execute(sql, args).fetchall()
        more = len(rows) > lim
        rows = rows[:lim]
        out, invalid, last = [], 0, None
        for row in rows:
            record_id = row["key"][len(prefix):]
            if not record_id:
                continue
            last = record_id
            try:
                rec = self._load(kind, record_id)
            except FailoverStateError:
                invalid += 1
                continue
            if rec is None:
                continue
            if project is not None and rec.get("project") != project:
                continue
            if state is not None and rec.get("status") != state:
                continue
            out.append(rec)
        return {"records": out, "more": more, "next_after_id": last if more else None,
                "invalid_records": invalid}

    # ---- 版本化策略 ----------------------------------------------------------
    def _policy_key(self, scope: str, scope_id: str) -> str:
        return f"{POLICY_PREFIX}{scope}:{scope_id}"

    def get_policy(self, scope: str, scope_id: str) -> dict | None:
        raw = meta_get(self.conn, self._policy_key(scope, scope_id))
        if not self._read_schema():
            return None
        raw = meta_get(self.conn, self._policy_key(scope, scope_id))
        if raw is None:
            return None
        policy = json_loads(raw, default=None)
        if not isinstance(policy, dict):
            raise FailoverStateError("failover 策略记录损坏：非对象")
        _validate_policy(policy)
        return policy

    def set_policy(self, scope: str, scope_id: str, policy: dict, *,
                   expected_revision: int | None,
                   operation_id: str | None = None,
                   reason: str | None = None,
                   actor: str = "operator") -> dict:
        """版本化策略写入（CAS）。expected_revision=None → 仅创建。"""
        if not (_nk(scope) and _nk(scope_id)):
            raise ValueError("scope/scope_id 必须为非空字符串")
        if operation_id is not None and not _nk(operation_id):
            raise ValueError("operation_id 必须为非空字符串")
        if reason is not None and not _nk(reason):
            raise ValueError("reason 必须为非空字符串")
        if not _nk(actor):
            raise ValueError("actor 必须为非空字符串")
        now = self._now_ms()
        key = self._policy_key(scope, scope_id)
        with transaction(self.conn):
            self._require_guard()
            self._ensure_schema()
            existing = self.get_policy(scope, scope_id)
            if existing is not None and operation_id is not None \
                    and existing.get("last_operation_id") == operation_id:
                candidate = {**existing, **policy}
                candidate.pop("revision", None)
                comparison = {key: value for key, value in existing.items()
                              if key not in ("revision", "created_at", "updated_at",
                                             "updated_by", "update_reason",
                                             "last_operation_id")}
                proposed = {key: value for key, value in candidate.items()
                            if key not in ("created_at", "updated_at",
                                           "updated_by", "update_reason",
                                           "last_operation_id")}
                if comparison == proposed:
                    return {"created": False, "replayed": True,
                            "policy": existing, "revision": existing["revision"]}
                raise FailoverStateError("failover operation_id 已用于不同策略")
            if existing is None:
                if expected_revision is not None:
                    raise FailoverStateError(
                        f"failover 策略 CAS 冲突：不存在但期望 {expected_revision}")
                rec = {"schema": FAILOVER_SCHEMA, "scope": scope, "scope_id": scope_id,
                       "revision": 1, "created_at": now, "updated_at": now, **policy}
                created = True
            else:
                if expected_revision is None:
                    raise FailoverStateError(
                        "failover 策略 CAS 冲突：已存在且未给期望版本")
                if int(existing["revision"]) != int(expected_revision):
                    raise FailoverStateError(
                        f"failover 策略 CAS 冲突：期望 {expected_revision}，"
                        f"实际 {existing['revision']}")
                rec = {**existing, **policy, "scope": scope, "scope_id": scope_id,
                       "revision": int(existing["revision"]) + 1,
                       "created_at": existing["created_at"], "updated_at": now}
                created = False
            rec.update({"updated_by": actor,
                        "update_reason": str(reason or "")[:300],
                        "last_operation_id": operation_id})
            _validate_policy(rec)
            meta_set(self.conn, key, json_dumps(rec))
        self._audit_log("failover.policy." + ("create" if created else "update"),
                        "policy", f"{scope}:{scope_id}",
                        {"revision": rec["revision"], "enabled": rec.get("enabled"),
                         "operation_id": operation_id, "reason": str(reason or "")[:200]})
        return {"created": created, "replayed": False,
                "policy": rec, "revision": rec["revision"]}

    # ---- 原路线硬故障阻断 ----------------------------------------------------
    def set_route_blocked(self, route: str, *, reason: str, until_ms: int | None,
                          now: int | None = None) -> dict:
        """登记原路线硬故障阻断。until_ms=None → 永久阻断（F24：时间流逝不解锁）。"""
        if not _nk(route):
            raise ValueError("route 必须为非空字符串")
        now = now if isinstance(now, int) else self._now_ms()
        key = f"{ROUTE_BLOCK_PREFIX}{route}"
        entry = {"schema": FAILOVER_SCHEMA, "route": route, "reason": reason[:500],
                 "until_ms": until_ms, "blocked_at": now, "revision": 1}
        with transaction(self.conn):
            self._require_guard()
            self._ensure_schema()
            raw = meta_get(self.conn, key)
            prev = json_loads(raw, default=None)
            if isinstance(prev, dict):
                entry["revision"] = int(prev.get("revision") or 0) + 1
            meta_set(self.conn, key, json_dumps(entry))
        self._audit_log("failover.route-block.set", "route", route,
                        {"reason": reason[:200], "until_ms": until_ms})
        return dict(entry)

    def is_route_blocked(self, route: str, *, now: int | None = None) -> dict | None:
        if not self._read_schema():
            return None
        raw = meta_get(self.conn, f"{ROUTE_BLOCK_PREFIX}{route}")
        if raw is None and "#" in route:
            raw = meta_get(self.conn, f"{ROUTE_BLOCK_PREFIX}{route.split('#', 1)[0]}")
        if raw is None:
            return None
        entry = json_loads(raw, default=None)
        if not isinstance(entry, dict) or not _nk(entry.get("route")):
            raise FailoverStateError("failover 路线阻断记录损坏")
        now = now if isinstance(now, int) else self._now_ms()
        if entry.get("cleared") is True:
            return {**entry, "blocked": False, "cleared": True}
        until = entry.get("until_ms")
        if isinstance(until, int) and not isinstance(until, bool) and until > 0 \
                and now >= until:
            # F24：时间流逝不等于解除。观察窗到期只作状态提示，派单仍须显式
            # 清除阻断（具备独立恢复证据或人工理由）。
            return {**entry, "blocked": True, "expired": True}
        return {**entry, "blocked": True, "expired": False}

    def clear_route_blocked(self, route: str, *, reason: str,
                            now: int | None = None) -> dict:
        """仅由明确恢复证据或人工处置解除；返回是否确有清除。"""
        if not _nk(route):
            raise ValueError("route 必须为非空字符串")
        now = now if isinstance(now, int) else self._now_ms()
        key = f"{ROUTE_BLOCK_PREFIX}{route}"
        with transaction(self.conn):
            self._require_guard()
            self._ensure_schema()
            raw = meta_get(self.conn, key)
            if raw is None:
                return {"cleared": False, "reason": "not-blocked"}
            entry = json_loads(raw, default=None)
            if not isinstance(entry, dict):
                raise FailoverStateError("failover 路线阻断记录损坏")
            entry["cleared"] = True
            entry["cleared_at"] = now
            entry["cleared_reason"] = reason[:500]
            entry["revision"] = int(entry.get("revision") or 0) + 1
            meta_set(self.conn, key, json_dumps(entry))
        self._audit_log("failover.route-block.clear", "route", route,
                        {"reason": reason[:200]})
        return {"cleared": True, "reason": reason, "revision": entry["revision"]}

    # ---- 游标（恢复调谐公平扫描，只由用例推进） -----------------------------
    def get_cursor(self, kind: str) -> str | None:
        if not self._read_schema():
            return None
        raw = meta_get(self.conn, f"{CURSOR_PREFIX}{kind}")
        return raw if isinstance(raw, str) and raw else None

    def set_cursor(self, kind: str, value: str | None) -> None:
        key = f"{CURSOR_PREFIX}{kind}"
        with transaction(self.conn):
            self._require_guard()
            self._ensure_schema()
            meta_set(self.conn, key, "" if value is None else value)

    # ---- 只读投影/能力守卫 ---------------------------------------------------
    def guard_valid(self) -> dict:
        """只读能力写契约与自有记录的有效性投影。"""
        schema = meta_get(self.conn, FAILOVER_SCHEMA_KEY)
        invalid = 0
        try:
            schema = FAILOVER_SCHEMA if str(schema) == str(FAILOVER_SCHEMA) else None
        except (TypeError, ValueError):
            schema = None
            invalid += 1
        try:
            self.list_records("episode", limit=1)
        except FailoverStateError:
            invalid += 1
        try:
            self.list_records("operation", limit=1)
        except FailoverStateError:
            invalid += 1
        try:
            guard = check_failover_guard(self.conn)
            guard_error = None
        except Exception as exc:
            guard, guard_error = False, str(exc)
        return {"schema": schema or None,
                "valid": bool(guard and schema is not None and invalid == 0),
                "guard_installed": bool(guard), "guard_error": guard_error,
                "invalid_records": invalid}

    def new_id(self, prefix: str, length: int = 12) -> str:
        return short_id(prefix, length)

    # ---- failover 事实 source 校验（供 EmailOutbox 注入） --------------------
    def recheck_failover_binding(self, conn: sqlite3.Connection,
                                 binding: dict) -> dict:
        """failover 事实验证（active/unknown；事实知会不因路由换代失效，N08）。

        - ``failover-episode``：episode 记录存在且归属与绑定一致 -> active；
          记录损坏/缺失/归属不符 -> unknown（fail-closed）。事件版本只作审计，
          不因 episode 前进为 stale（已发生事实即便恢复也保留知会，N07/N08）。
        - ``failover-operation``（恢复事件）：操作存在且关联 episode 已 resumed
          -> active（可发恢复摘要）；否则 unknown。
        """
        if not isinstance(binding, dict):
            return _v(False, "unknown", "invalid-binding")
        kind = binding.get("source_kind")
        sid = binding.get("source_id")
        if not (_nk(kind) and _nk(sid)):
            return _v(False, "unknown", "binding-fields-invalid")
        if kind == "failover-episode":
            try:
                rec = self._load_with(conn, "episode", sid)
            except FailoverStateError:
                return _v(False, "unknown", "record-corrupt")
            if rec is None:
                return _v(False, "unknown", "episode-missing")
            if binding.get("source_project") not in (None, rec.get("project")):
                return _v(False, "unknown", "project-mismatch")
            if binding.get("source_task_id") not in (None, rec.get("task_id")):
                return _v(False, "unknown", "task-mismatch")
            if binding.get("source_session") not in (None, rec.get("session_id")):
                return _v(False, "unknown", "session-mismatch")
            return _v(True, "active", "verified")
        if kind == "failover-operation":
            try:
                op = self._load_with(conn, "operation", sid)
            except FailoverStateError:
                return _v(False, "unknown", "record-corrupt")
            if op is None:
                return _v(False, "unknown", "operation-missing")
            ep_id = op.get("episode_id")
            if not _nk(ep_id):
                return _v(False, "unknown", "operation-episode-missing")
            try:
                ep = self._load_with(conn, "episode", ep_id)
            except FailoverStateError:
                return _v(False, "unknown", "record-corrupt")
            if ep is None or ep.get("status") != "resumed":
                return _v(False, "unknown", "episode-not-resumed")
            return _v(True, "active", "verified-resumed")
        return _v(False, "unknown", "unsupported-source-kind")

    def _load_with(self, conn: sqlite3.Connection, kind: str,
                   record_id: str) -> dict | None:
        """与 _load 同规则，但用给定 conn（与 outbox validator 同一事务）。"""
        raw = meta_get(conn, _record_key(kind, record_id))
        if raw is None:
            return None
        rec = json_loads(raw, default=None)
        if not isinstance(rec, dict):
            raise FailoverStateError(f"failover 记录损坏（{kind}/{record_id}）：非对象")
        _validate_record(rec)
        return rec

    _BINDING_FLAT_KEYS = ("source_kind", "source_id", "source_condition_key",
                            "source_version", "source_state",
                            "source_incident_type", "source_project",
                            "source_task_id", "source_session", "source_model")

    def make_source_validator(self) -> Any:
        """兼容 EmailOutbox.source_validator 窄端口：(conn, item) -> verdict。

        与 operator_incident 同构：outbox 把 source_binding 扁平化到行字段，
        先从行重建绑定（item 无 source_binding 键），再按 failover 归属重验。
        """

        def validate(conn, item):
            binding = None
            if isinstance(item, dict):
                binding = item.get("source_binding")
                if not isinstance(binding, dict) and item.get("source_kind"):
                    binding = {k: item.get(k) for k in self._BINDING_FLAT_KEYS}
            if not isinstance(binding, dict):
                return _v(False, "unknown", "source-binding-missing")
            try:
                return self.recheck_failover_binding(conn, binding)
            except (FailoverStateError, ValueError, TypeError):
                return _v(False, "unknown", "recheck-read-failed")

        return validate


def _v(ok: bool, state: str, reason: str) -> dict:
    """failover outbox 判定辅助（state/ok/reason 与 operator_incident 同构）。"""
    return {"state": state, "ok": bool(ok), "reason": reason}


__all__ = ["FailoverLedger", "FailoverStateError", "failover_episode_id",
           "FAILOVER_SCHEMA", "FAILOVER_SCHEMA_KEY"]

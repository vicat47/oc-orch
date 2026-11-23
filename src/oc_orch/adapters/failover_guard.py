"""提供商故障恢复独立写契约与 session 恢复占用守卫。

安装是显式、原子且可审计的；默认 init/connect 不安装契约。安装后，旧连接因
不认识本模块的 SQLite UDF 而 fail closed。恢复占用通过 meta 中的 session lock
拦截其它 Attempt 和托管输入写入；唯一获准的恢复派单在连接局部 operation
context 中执行。
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import sqlite3
from typing import Iterator

from ..util import json_dumps, now_ms
from ..domain.errors import FailoverGuardError
from .sqlite_db import meta_get, meta_set, transaction
from .writer_guard import check_writer_guard

KEY = "failover:writer-contract"
VERSION = "provider-failover-v1"
FUNCTION = "oc_orch_failover_operation"
PREFIX = "oc_orch_failover_v1_"
LOCK_PREFIX = "failover:session-lock:"
NO_OPERATION = "__no_failover_operation__"

_CONTEXT: ContextVar[tuple[int, str] | None] = ContextVar(
    "oc_orch_failover_operation_context", default=None)


def _trigger_sql(table: str, operation: str) -> tuple[str, str]:
    name = f"{PREFIX}{table}_{operation.lower()}"
    locked_elsewhere = (
        f"EXISTS (SELECT 1 FROM meta WHERE key='{LOCK_PREFIX}' || "
        f"COALESCE(NEW.session_id,'') AND value<>{FUNCTION}())")
    if operation == "UPDATE":
        locked_elsewhere = (
            f"({locked_elsewhere} OR EXISTS (SELECT 1 FROM meta "
            f"WHERE key='{LOCK_PREFIX}' || COALESCE(OLD.session_id,'') "
            f"AND value<>{FUNCTION}()))")
    if table == "attempts":
        unauthorized = f"{FUNCTION}() IS NULL OR {locked_elsewhere}"
        if operation == "UPDATE":
            fields = (
                "id", "task_id", "generation", "session_id", "directory",
                "agent", "model", "variant", "dispatch_msg_id", "deadline_at",
                "stall_after", "created_at")
            stable_successor = " AND ".join(
                f"NEW.{f} IS OLD.{f}" for f in fields)
            # Once prepare_continuation has durably fixed the successor identity,
            # that same attempt must be allowed to record execution/collection
            # facts while the session remains reserved. Its routing identity is
            # immutable; other attempts and managed inputs remain blocked.
            successor_update = (
                "EXISTS (SELECT 1 FROM meta AS lock_row "
                "JOIN meta AS operation_row ON operation_row.key="
                "'failover:operation:' || lock_row.value "
                "JOIN meta AS episode_row ON episode_row.key="
                "'failover:episode:' || json_extract(operation_row.value,'$.episode_id') "
                f"WHERE lock_row.key='{LOCK_PREFIX}' || COALESCE(NEW.session_id,'') "
                "AND json_extract(operation_row.value,'$.session_id')=NEW.session_id "
                "AND json_extract(episode_row.value,'$.metadata.successor_attempt_id')=NEW.id "
                f"AND {stable_successor})")
            safe_stop = " AND ".join(
                f"NEW.{f} IS OLD.{f}" for f in (
                    "id", "task_id", "generation", "session_id", "directory",
                    "agent", "model", "variant", "dispatch_state", "dispatch_msg_id",
                    "session_meta", "started_at", "dispatched_at", "last_progress_at",
                    "last_observed_at", "deadline_at", "stall_after", "terminal_at",
                    "terminal_kind", "terminal_reason", "stop_confirmed_at",
                    "site_released_at", "result_collected_at", "result_path",
                    "result_summary", "verification", "created_at", "metadata"))
            fields_release = tuple(f for f in (
                "id", "task_id", "generation", "session_id", "directory", "agent",
                "model", "variant", "dispatch_state", "dispatch_msg_id", "session_meta",
                "started_at", "dispatched_at", "last_progress_at", "last_observed_at",
                "deadline_at", "stall_after", "terminal_at", "terminal_kind",
                "terminal_reason", "result_collected_at", "result_path", "result_summary",
                "verification", "created_at") if f not in (
                    "stop_confirmed_at", "site_released_at"))
            safe_release = " AND ".join(f"NEW.{f} IS OLD.{f}" for f in fields_release)
            safe_release += " AND NEW.status IS OLD.status"
            manual = (
                f"((NEW.status='stop_requested' AND "
                f"NEW.stop_requested_at IS NOT OLD.stop_requested_at AND {safe_stop}) OR "
                f"(NEW.site_released_at IS NOT OLD.site_released_at AND "
                f"NEW.stop_confirmed_at IS NOT OLD.stop_confirmed_at AND {safe_release}))"
            )
            condition = (f"({unauthorized}) AND NOT ({manual} OR "
                         f"({successor_update}))")
        else:
            condition = unauthorized
        message = "OC_ORCH_FAILOVER_SESSION_OCCUPIED"
    else:
        unauthorized = f"{FUNCTION}() IS NULL OR {locked_elsewhere}"
        if operation == "UPDATE":
            fixed = ("input_id", "session_id", "message_id", "source", "intent",
                     "body", "body_hash", "created_at", "prepared_at")
            stable_identity = " AND ".join(
                f"NEW.{f} IS OLD.{f}" for f in fixed)
            manual_cancel = (
                f"(NEW.admission_state IN ('cancelled','delivered','released') AND "
                f"NEW.session_id IS OLD.session_id AND {stable_identity})")
            condition = f"({unauthorized}) AND NOT {manual_cancel}"
        else:
            condition = unauthorized
        message = "OC_ORCH_FAILOVER_SESSION_OCCUPIED"
    sql = (f'CREATE TRIGGER "{name}" BEFORE {operation} ON "{table}" '
           f"WHEN {condition} BEGIN SELECT RAISE(ABORT, '{message}'); END")
    return name, sql


def _expected() -> dict[str, str]:
    out: dict[str, str] = {}
    for table in ("attempts", "session_inputs"):
        for operation in ("INSERT", "UPDATE"):
            name, sql = _trigger_sql(table, operation)
            out[name] = sql
    return out


def check_failover_guard(conn: sqlite3.Connection) -> bool:
    """严格只读验证标记、所有触发器和底层 continuity-v2 契约。"""
    triggers = dict(conn.execute(
        "SELECT name,sql FROM sqlite_master WHERE type='trigger' "
        "AND name GLOB 'oc_orch_failover_*'"))
    has_meta = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
    marker = (conn.execute("SELECT value FROM meta WHERE key=?", (KEY,)).fetchone()
              if has_meta else None)
    if marker is None and not triggers:
        return False
    if (marker is None or marker[0] != VERSION or triggers != _expected()
            or not check_writer_guard(conn)):
        raise FailoverGuardError("provider failover 写契约未知或覆盖不完整")
    return True


def configure_failover_connection(conn: sqlite3.Connection) -> None:
    """绑定当前连接的临时 operation 权限，并拒绝损坏/未知契约。"""
    connection_id = id(conn)

    def current_operation() -> str:
        value = _CONTEXT.get()
        return value[1] if value is not None and value[0] == connection_id else NO_OPERATION

    conn.create_function(FUNCTION, 0, current_operation, deterministic=False)
    check_failover_guard(conn)


@contextmanager
def authorized_failover_operation(conn: sqlite3.Connection,
                                  operation_id: str) -> Iterator[None]:
    if not isinstance(operation_id, str) or not operation_id.strip():
        raise ValueError("operation_id 必须为非空字符串")
    token = _CONTEXT.set((id(conn), operation_id))
    try:
        yield
    finally:
        _CONTEXT.reset(token)


def install_failover_guard(conn: sqlite3.Connection, *, actor: str = "operator") -> dict:
    """显式在已安装 continuity-v2 的 schema3 台账上安装 failover 守卫。"""
    if not isinstance(actor, str) or not actor.strip():
        raise ValueError("actor 必须为非空字符串")
    if conn.in_transaction:
        raise RuntimeError("failover 写契约安装不得嵌套事务")
    try:
        bound = conn.execute(f"SELECT {FUNCTION}()").fetchone()[0]
    except sqlite3.OperationalError as exc:
        raise FailoverGuardError("连接未绑定 failover 写契约") from exc
    if bound != NO_OPERATION:
        raise FailoverGuardError("安装时不允许存在恢复 operation context")

    with transaction(conn):
        if check_failover_guard(conn):
            return {"installed": False, "version": VERSION, "triggers": 4}
        if not check_writer_guard(conn):
            raise FailoverGuardError("须先安装并验证 continuity-v2 写契约")
        for _, sql in _expected().items():
            conn.execute(sql)
        conn.execute("INSERT INTO meta(key,value) VALUES(?,?)", (KEY, VERSION))
        meta_set(conn, "failover:schema", "1")
        conn.execute(
            "INSERT INTO audit(at,actor,action,entity,entity_id,from_state,to_state,detail) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (now_ms(), actor, "failover-contract.install", "schema", "3", None,
             VERSION, json_dumps({"triggers": 4, "business_rows_changed": False})))
        check_failover_guard(conn)
    return {"installed": True, "version": VERSION, "triggers": 4}


def session_lock_key(session_id: str) -> str:
    if not isinstance(session_id, str) or not session_id.strip():
        raise ValueError("session_id 必须为非空字符串")
    return LOCK_PREFIX + session_id


__all__ = [
    "FailoverGuardError", "KEY", "VERSION", "FUNCTION", "PREFIX",
    "LOCK_PREFIX", "NO_OPERATION", "authorized_failover_operation",
    "check_failover_guard", "configure_failover_connection",
    "install_failover_guard", "session_lock_key",
]

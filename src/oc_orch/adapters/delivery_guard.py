"""交付策略独立写契约；叠加在continuity-v2，不改旧v1→v2迁移语义。

只有显式安装才启用运行策略。旧v2的连接不注册本UDF，安装后连已打开的
旧连接也不能DML；读取仍可用。不是对可删trigger的数据库所有者的隔离。
"""

from __future__ import annotations

import sqlite3

from ..domain.errors import LedgerError
from ..util import json_dumps, now_ms
from .sqlite_db import transaction
from .writer_guard import TABLES, OPERATIONS, check_writer_guard

KEY = "delivery-policy:writer-contract"
VERSION = "delivery-policy-v1"
FUNCTION = "oc_orch_delivery_contract"
PREFIX = "oc_orch_delivery_v1_"


def _expected() -> dict[str, str]:
    return {f"{PREFIX}{table}_{op.lower()}":
            f'CREATE TRIGGER "{PREFIX}{table}_{op.lower()}" BEFORE {op} ON "{table}" '
            f"WHEN {FUNCTION}() IS NOT '{VERSION}' "
            "BEGIN SELECT RAISE(ABORT, 'OC_ORCH_DELIVERY_WRITER_INCOMPATIBLE'); END"
            for table in TABLES for op in OPERATIONS}


def check_delivery_guard(conn: sqlite3.Connection) -> bool:
    """严格只读；无标记且无trigger为未启用，半残/未知一律拒绝。"""
    triggers = dict(conn.execute(
        "SELECT name,sql FROM sqlite_master WHERE type='trigger' "
        "AND name GLOB 'oc_orch_delivery_*'"))
    has_meta = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
    marker = conn.execute("SELECT value FROM meta WHERE key=?", (KEY,)).fetchone() if has_meta else None
    if marker is None and not triggers:
        return False
    if (marker is None or marker[0] != VERSION or triggers != _expected()
            or not check_writer_guard(conn)):
        raise LedgerError("DELIVERY_GUARD_INVALID", "交付策略写契约未知或覆盖不完整；拒绝发送/写入")
    return True


def configure_delivery_connection(conn: sqlite3.Connection) -> None:
    conn.create_function(FUNCTION, 0, lambda: VERSION, deterministic=True)
    check_delivery_guard(conn)


def install_delivery_guard(conn: sqlite3.Connection, *, actor: str = "operator") -> dict:
    """停写/一致备份后的显式安装；不纳管旧Task、不补造资源授权。"""
    if not isinstance(actor, str) or not actor.strip():
        raise ValueError("actor必须是非空字符串")
    with transaction(conn):
        if check_delivery_guard(conn):
            return {"installed": False, "version": VERSION, "triggers": 36}
        if not check_writer_guard(conn):
            raise LedgerError("DELIVERY_GUARD_BASE_REQUIRED", "须先显式安装完整continuity-v2契约")
        try:
            bound = conn.execute(f"SELECT {FUNCTION}()").fetchone()[0]
        except sqlite3.OperationalError as exc:
            raise LedgerError("DELIVERY_GUARD_CONNECTION_REQUIRED", "连接未绑定交付策略写契约") from exc
        if bound != VERSION:
            raise LedgerError("DELIVERY_GUARD_CONNECTION_REQUIRED", "连接写契约版本不符")
        for sql in _expected().values():
            conn.execute(sql)
        conn.execute("INSERT INTO meta(key,value) VALUES(?,?)", (KEY, VERSION))
        conn.execute(
            "INSERT INTO audit(at,actor,action,entity,entity_id,from_state,to_state,detail) "
            "VALUES(?,?,?,?,?,?,?,?)", (now_ms(), actor, "delivery-policy.install", "schema", "3",
            None, VERSION, json_dumps({"triggers": 36, "business_rows_changed": False,
                                      "legacy_tasks_adopted": False})))
        check_delivery_guard(conn)
    return {"installed": True, "version": VERSION, "triggers": 36}

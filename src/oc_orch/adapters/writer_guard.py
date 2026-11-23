"""显式发布的 schema3 写入契约门，不猜测/迁移历史控制或重播业务输入。

版本语义：

- ``continuity-v1``（已上线初版）：旧实现/旧连接注册的函数返回 ``'continuity-v1'``，
  36 个 v1 trigger 校验该值。
- ``continuity-v2``（本开发版）：默认 ``connect/init`` 只承认 v2。遇**完整 v1**
  台账在打开时由 ``check_writer_guard`` 明确拒写、**不自动升级、无副作用**；
  部署侧停写/备份后，经窄 API ``upgrade_writer_guard(conn, actor=...)`` 做
  一次性 v1→v2 原子替换。

安全不变量：

1. 触发器**前缀沿用**（``oc_orch_writer_v1_``），但每个 trigger 的 SQL 内容按
   版本**精确匹配**：v1 内容校验 ``'continuity-v1'``，v2 内容校验
   ``'continuity-v2'``。``check_writer_guard`` 以完整 SQL 字面量比较，不以名称
   放行。
2. 任何 DML 都必须过 trigger：函数未注册 → ``no such function``；函数返回值
   ≠ 当前版本 → ``RAISE(ABORT, 'OC_ORCH_WRITER_INCOMPATIBLE')``。因此**旧已
   打开、已注册返回 ``'continuity-v1'`` 函数的 v1 连接，在库升级为 v2 后会被
   v2 trigger 拒写**，不能继续混跑。
3. ``upgrade_writer_guard`` 只对“原始连接（可能无 UDF）”做**严格只读确认**
   完整 v1（schema3 + v1 标记 + 36 个精确 v1 trigger），随后在同一
   ``BEGIN IMMEDIATE`` 内**重验**并原子替换：DROP 36 个 v1 trigger + CREATE
   36 个 v2 trigger + marker 更新 + 恰 1 条 ``writer-contract.upgrade`` audit。
   不通过“删全部保护后事务外重开”；任何注入失败整体回滚回完整 v1，旧事实可读。
4. 未知标记 / 半残 v1 / 未来版本：拒绝升级，**不修复破损、不删保护、不倒库**。
   v2 已安装时重复升级返回 ``upgraded=False`` 且**零额外写入**。
5. 这不是针对数据库所有者的安全隔离：发布仍须停旧进程、统一入口；能自行删
   trigger/注册函数的任意 SQL 客户端不在本门信任边界内。
"""

from __future__ import annotations

import re
import sqlite3

from ..util import json_dumps, now_ms

GUARD_KEY = "writer-contract:version"
GUARD_VERSION = "continuity-v2"
V1_GUARD_VERSION = "continuity-v1"
FUNCTION = "oc_orch_writer_contract"
PREFIX = "oc_orch_writer_v1_"
TABLES = (
    "alerts", "attempts", "audit", "coordinator_leases", "deliverables",
    "events", "meta", "notifications", "responsibilities", "session_inputs",
    "tasks", "turns",
)
OPERATIONS = ("INSERT", "UPDATE", "DELETE")


class WriterGuardStateError(RuntimeError):
    """持久写契约未知或覆盖不完整，拒绝打开写连接。"""


def _trigger_sql(table: str, operation: str, version: str = GUARD_VERSION) -> str:
    name = f"{PREFIX}{table}_{operation.lower()}"
    return (f'CREATE TRIGGER "{name}" BEFORE {operation} ON "{table}" '
            f"WHEN {FUNCTION}() IS NOT '{version}' "
            "BEGIN SELECT RAISE(ABORT, 'OC_ORCH_WRITER_INCOMPATIBLE'); END")


def _expected_triggers() -> dict[str, str]:
    """当前（v2）精确 trigger SQL 契约。"""
    return {f"{PREFIX}{table}_{op.lower()}": _trigger_sql(table, op)
            for table in TABLES for op in OPERATIONS}


def _expected_v1_triggers() -> dict[str, str]:
    """已上线 v1 版本的精确 trigger SQL 契约（只读参考冻结生产快照）。"""
    return {f"{PREFIX}{table}_{op.lower()}": _trigger_sql(table, op, V1_GUARD_VERSION)
            for table in TABLES for op in OPERATIONS}


def _tables_of(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%'")}


def _triggers_of(conn: sqlite3.Connection) -> dict[str, str]:
    return dict(conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='trigger' "
        "AND name GLOB 'oc_orch_writer_*'").fetchall())


def _schema3_ok(conn: sqlite3.Connection, tables: set[str]) -> bool:
    row = conn.execute(
        "SELECT value FROM meta WHERE key='schema_version'").fetchone()
    return bool(row and str(row[0]) == "3" and tables == set(TABLES))


def _detect_contract(conn: sqlite3.Connection) -> str:
    """只读分类当前写契约状态（不注册函数、不写库、不抛异常）。

    返回：'v2' | 'v1' | 'unguarded' | 'broken' | 'unknown' | 'future'。
    - ``v2``：完整当前契约（schema3 + v2 标记 + 36 个精确 v2 trigger）；
    - ``v1``：完整已上线契约（schema3 + v1 标记 + 36 个精确 v1 trigger）；
    - ``unguarded``：schema3 且无标记、无 trigger（可 install，不应 upgrade）；
    - ``broken``：有标记/trigger 但覆盖不完整，或 meta/标记缺失但已装 trigger；
    - ``unknown``：标记存在但不是已知 v1/v2 且不是 ``continuity-vN`` 形态；
    - ``future``：标记为 ``continuity-vN`` 但非当前/已上线版本。
    """
    has_meta = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'"
    ).fetchone()
    triggers = _triggers_of(conn)
    if has_meta is None:
        return "broken" if triggers else "unguarded"
    tables = _tables_of(conn)
    marker = conn.execute(
        "SELECT value FROM meta WHERE key=?", (GUARD_KEY,)).fetchone()
    marker = marker[0] if marker is not None else None
    schema3 = _schema3_ok(conn, tables)
    if marker == GUARD_VERSION:
        return "v2" if schema3 and triggers == _expected_triggers() else "broken"
    if marker == V1_GUARD_VERSION:
        return "v1" if schema3 and triggers == _expected_v1_triggers() else "broken"
    if marker is None:
        return "broken" if triggers else "unguarded"
    if isinstance(marker, str) and re.fullmatch(r"continuity-v\d+", marker):
        return "future"
    return "unknown"


def check_writer_guard(conn: sqlite3.Connection) -> bool:
    """只读核验**当前（v2）**持久标记与全部 trigger 的精确内容；未安装返回 False。"""
    triggers = _triggers_of(conn)
    has_meta = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
    marker = (conn.execute("SELECT value FROM meta WHERE key=?", (GUARD_KEY,)).fetchone()
              if has_meta else None)
    if marker is None:
        if triggers:
            raise WriterGuardStateError("写入契约标记缺失；拒绝放行残缺的写保护")
        return False
    if marker[0] != GUARD_VERSION:
        raise WriterGuardStateError("不兼容的台账写入契约，拒绝旧/未知写入者")
    if _tables_of(conn) != set(TABLES) or triggers != _expected_triggers():
        raise WriterGuardStateError("台账写入契约覆盖不完整或已改变，拒绝写入")
    return True


def configure_writer_connection(conn: sqlite3.Connection) -> None:
    """新实现的每个连接都绑定相同（v2）契约；不自动安装或改动业务数据。"""
    conn.create_function(FUNCTION, 0, lambda: GUARD_VERSION, deterministic=True)
    check_writer_guard(conn)


def install_writer_guard(conn: sqlite3.Connection, *, actor: str = "operator") -> dict:
    """显式短事务安装当前（v2）契约；中断整体回滚。重复安装不新增审计。

    仅用于“未安装写契约的完整 schema3 台账”（如发布副本/新的空台账）。
    已安装 v2 → 返回 ``installed=False`` 且零额外写入；完整 v1 → 抛
    :class:`WriterGuardStateError`（须用 :func:`upgrade_writer_guard` 升级）。
    """
    from .sqlite_db import transaction

    if not isinstance(actor, str) or not actor.strip():
        raise ValueError("actor 必须是非空字符串")
    if conn.in_transaction:
        raise RuntimeError("写入契约安装不得嵌套事务")
    with transaction(conn):
        if check_writer_guard(conn):
            return {"installed": False, "version": GUARD_VERSION, "triggers": 36}
        version = conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'").fetchone()
        tables = _tables_of(conn)
        bound = conn.execute(f"SELECT {FUNCTION}()").fetchone()[0]
        if not version or version[0] != "3" or tables != set(TABLES) or bound != GUARD_VERSION:
            raise WriterGuardStateError("仅支持已绑定新写入者的完整 schema3 台账")
        for sql in _expected_triggers().values():
            conn.execute(sql)
        conn.execute("INSERT INTO meta(key,value) VALUES(?,?)", (GUARD_KEY, GUARD_VERSION))
        conn.execute(
            "INSERT INTO audit(at,actor,action,entity,entity_id,from_state,to_state,detail) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (now_ms(), actor, "writer-contract.install", "schema", "3", None,
             GUARD_VERSION, json_dumps({"triggers": 36, "business_rows_changed": False})))
        check_writer_guard(conn)
    return {"installed": True, "version": GUARD_VERSION, "triggers": 36}


_UPGRADE_REJECT_REASON = {
    "unguarded": "台账未安装写契约：应显式 install_writer_guard，而非升级",
    "broken": "写契约标记/触发器不完整或已改变：拒绝升级，不修复破损、不删保护",
    "unknown": "写入契约标记未知：拒绝升级",
    "future": "写入契约为未来版本：拒绝降级或未知版本",
}


def upgrade_writer_guard(conn: sqlite3.Connection, *, actor: str = "operator") -> dict:
    """显式 v1→v2 写契约升级（部署时停写/一致备份后调用）。

    - 原始连接允许**无 UDF**：只读确认与替换都按 SQL 字面量进行，不依赖函数
      注册；升级完成后才注册 v2 函数做“真实 SQL 函数版本绑定”的最终核验；
    - 完整 v1（schema3 + ``continuity-v1`` 标记 + 36 个精确 v1 trigger）才可
      升级；同一 ``BEGIN IMMEDIATE`` 内重验后原子替换为 v2（DROP 36 个 v1 +
      CREATE 36 个 v2 + marker 更新 + 恰 1 条 ``writer-contract.upgrade`` audit），
      任何注入失败整体回滚回完整 v1，旧事实可读；
    - 已 v2 → ``{"upgraded": False, ...}`` **零额外写入**；未知/半残 v1/未来
      版本 → 抛 :class:`WriterGuardStateError`，不修复、不倒库。
    """
    from .sqlite_db import transaction

    if not isinstance(actor, str) or not actor.strip():
        raise ValueError("actor 必须是非空字符串")
    if conn.in_transaction:
        raise RuntimeError("写入契约升级不得嵌套事务")
    state = _detect_contract(conn)
    if state == "v2":
        return {"upgraded": False, "version": GUARD_VERSION, "triggers": 36}
    if state != "v1":
        raise WriterGuardStateError(_UPGRADE_REJECT_REASON.get(state, state))
    with transaction(conn):
        # 事务内重验：防止只读 probe 之后、持有写锁之前契约被并发改变。
        if _detect_contract(conn) != "v1":
            raise WriterGuardStateError("升级事务内写入契约已变化；整体回滚")
        # 按真实 SQL 函数版本绑定：先注册 v2 函数（连接本地、事务无关），
        # 使事务内对业务表的写入（marker 更新）直接过 v2 trigger 的真实校验；
        # 若事务回滚，该连接后续 DML 仍会被 v1 trigger 以 v2 函数值拒写（fail-closed）。
        conn.create_function(FUNCTION, 0, lambda: GUARD_VERSION, deterministic=True)
        for name in _expected_v1_triggers():
            conn.execute(f'DROP TRIGGER IF EXISTS "{name}"')
        for sql in _expected_triggers().values():
            conn.execute(sql)
        conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (GUARD_KEY, GUARD_VERSION))
        conn.execute(
            "INSERT INTO audit(at,actor,action,entity,entity_id,from_state,to_state,detail) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (now_ms(), actor, "writer-contract.upgrade", "schema", "3",
             V1_GUARD_VERSION, GUARD_VERSION,
             json_dumps({"triggers": 36, "business_rows_changed": False,
                         "v1_triggers_dropped": 36})))
        # 最终核验；失败整体回滚回完整 v1。
        if not check_writer_guard(conn):
            raise WriterGuardStateError("升级后写契约核验失败；整体回滚")
    return {"upgraded": True, "version": GUARD_VERSION, "triggers": 36}
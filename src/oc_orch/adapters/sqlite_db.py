"""SQLite 台账适配：单文件、WAL、显式事务。

兼容旧导入路径 oc_orch.db（root db.py 为 sys.modules 别名指向本模块）；本文件
是 SQLite 连接/schema/事务原语的单一权威实现。所有业务状态写入都应通过
Ledger（adapters.sqlite_ledger）；本模块只负责连接、schema 版本与迁移、事务
原语。

版本策略（2026-11-09 S2）：
- **先版本检查后建 schema**：任何 DDL 之前先读 meta.schema_version。版本一致
  才幂等确保表结构；future schema 一律拒绝（无副作用、连接直接 close）。
  修复旧实现“先 executescript 再版本检查”会先建未来 schema 再拒绝的风险。
- **显式 1→2 迁移**：新增 session_inputs 输入回执表（未决自动唤醒/物理发送
  身份/批次成员）。旧 notifications.message_id 非空且未消费的行登记为待核实
  unknown 历史回执（outstanding=0），不判定 delivered、不批量重放。
- **显式 2→3 迁移（2026-11-11 S4-A）**：新增 responsibilities 后续责任表
  （托管未完成工作的责任、等待条件、接续目标、预算与期限）。2→3 只做 DDL 与
  版本/审计；**不回填、不猜测历史输入执行事实、不自动重播**。缺归属的历史
  工作由上层按待诊断事实自行登记，迁移本身不造责任行。
- **1→2→3 链式升级**：v1 台账在同一 BEGIN IMMEDIATE 内先 1→2 再 2→3；
  中途失败整体回滚到 v1（历史 1→2 断言保持：版本仍 1、无 session_inputs、
  旧回执不被判定 delivered）。
- **迁移失败整体回滚**：表创建、历史回执导入、唯一索引、版本号写入在同一
  BEGIN IMMEDIATE；任何一步失败回滚并关闭连接。旧二进制（expect_version
  过旧）或未来版本打开一律在 DDL 前拒绝。
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from typing import Callable, Iterator

from .. import SCHEMA_VERSION
from ..util import ensure_home, json_dumps, now_ms, sha256_text
import time

# ---- S2 输入回执增量 DDL ----------------------------------------------
# 同一目标最多一个持久未决自动唤醒：session_inputs(session_id) 部分唯一索引
# WHERE outstanding=1（DB 级约束，不依赖 HTTP 后写 sent）。
_SESSION_INPUTS_TABLE = """
CREATE TABLE IF NOT EXISTS session_inputs (
  input_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  message_id TEXT,
  source TEXT NOT NULL DEFAULT 'automatic_wake',
  intent TEXT NOT NULL DEFAULT 'execute',
  outstanding INTEGER NOT NULL DEFAULT 0,
  revision INTEGER NOT NULL DEFAULT 1,
  owner TEXT,
  body TEXT NOT NULL DEFAULT '',
  body_hash TEXT NOT NULL DEFAULT '',
  notification_ids TEXT NOT NULL DEFAULT '[]',
  admission_state TEXT NOT NULL,
  claim_owner TEXT,
  claim_at INTEGER,
  prepared_at INTEGER NOT NULL,
  possible_at INTEGER,
  admitted_at INTEGER,
  delivered_at INTEGER,
  executed_at INTEGER,
  ack_deadline_at INTEGER,
  observed_at INTEGER,
  retry_count INTEGER NOT NULL DEFAULT 0,
  send_count INTEGER NOT NULL DEFAULT 0,
  next_reconcile_at INTEGER,
  reconcile_deadline INTEGER,
  nudge_count INTEGER NOT NULL DEFAULT 0,
  prior_input_id TEXT,
  replaced_by TEXT,
  metadata TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
"""

_SESSION_INPUTS_BASE_INDEXES = (
    "CREATE INDEX IF NOT EXISTS session_inputs_message "
    "  ON session_inputs(message_id);",
    "CREATE INDEX IF NOT EXISTS session_inputs_receipt "
    "  ON session_inputs(session_id, message_id);",
    "CREATE INDEX IF NOT EXISTS session_inputs_pending "
    "  ON session_inputs(session_id, admission_state);",
)

_SESSION_INPUTS_OUTSTANDING_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS session_inputs_outstanding "
    "  ON session_inputs(session_id) WHERE outstanding = 1;"
)

# 1→2 显式迁移：先表与普通索引，历史回执导入后，最后建未决唯一索引。
_SCHEMA_V2_DELTA = (
    _SESSION_INPUTS_TABLE
    + "".join(_SESSION_INPUTS_BASE_INDEXES)
)

# ---- S4-A 后续责任增量 DDL（schema 3） --------------------------------
# 一个责任 = “谁在等待什么、什么事实到达后交给谁处理”。与 Task/Attempt/
# Notification 分离：Notification 只是提醒渠道，其 ACK 不等于责任完成。
# 未关闭责任上的 (task_id, condition_key, generation, result_version) 唯一，
# 保证“同一条件的缺失责任诊断”不会每轮新建业务身份。
_RESPONSIBILITIES_TABLE = """
CREATE TABLE IF NOT EXISTS responsibilities (
  id TEXT PRIMARY KEY,
  project TEXT NOT NULL,
  task_id TEXT NOT NULL,
  attempt_id TEXT,
  generation INTEGER,
  result_version TEXT,
  input_id TEXT,
  session_id TEXT,
  message_id TEXT,
  owner TEXT NOT NULL,
  target_session TEXT,
  state TEXT NOT NULL,
  wait_kind TEXT NOT NULL,
  wait_condition TEXT NOT NULL,
  next_action TEXT NOT NULL,
  next_check_at INTEGER NOT NULL,
  deadline INTEGER,
  condition_key TEXT NOT NULL,
  revision INTEGER NOT NULL DEFAULT 1,
  budget_used INTEGER NOT NULL DEFAULT 0,
  max_budget INTEGER NOT NULL DEFAULT 3,
  evidence TEXT NOT NULL DEFAULT '{}',
  prior_id TEXT,
  successor_id TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  closed_at INTEGER,
  close_reason TEXT
);
"""

_RESPONSIBILITIES_INDEXES = (
    "CREATE INDEX IF NOT EXISTS responsibilities_open "
    "  ON responsibilities(task_id, state, closed_at);",
    "CREATE INDEX IF NOT EXISTS responsibilities_due "
    "  ON responsibilities(closed_at, next_check_at);",
    "CREATE INDEX IF NOT EXISTS responsibilities_owner "
    "  ON responsibilities(owner, closed_at);",
    "CREATE INDEX IF NOT EXISTS responsibilities_source "
    "  ON responsibilities(input_id, session_id);",
)

# 条件唯一：NULL 用 ifnull 归一（SQLite 唯一索引把 NULL 视作彼此不同，不能
# 直接依赖），只约束未关闭行；已关闭/已转交的历史行可重复保留。
_RESPONSIBILITIES_CONDITION_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS responsibilities_condition "
    "  ON responsibilities(task_id, condition_key, ifnull(generation, -1), "
    "  ifnull(result_version, '')) WHERE closed_at IS NULL;"
)

_SCHEMA_V3_DELTA = (
    _RESPONSIBILITIES_TABLE
    + "".join(_RESPONSIBILITIES_INDEXES)
    + _RESPONSIBILITIES_CONDITION_INDEX
)

# 新库直接建 v3 完整 schema（v1 表 + session_inputs 增量 + responsibilities 增量）。
SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  project TEXT NOT NULL,
  title TEXT NOT NULL,
  objective TEXT NOT NULL DEFAULT '',
  acceptance TEXT NOT NULL DEFAULT '[]',
  status TEXT NOT NULL,
  coordinator_session TEXT,
  write_scope TEXT NOT NULL DEFAULT '[]',
  priority INTEGER NOT NULL DEFAULT 0,
  next_check_at INTEGER,
  current_attempt_id TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  accepted_at INTEGER,
  closed_at INTEGER,
  blocked_reason TEXT,
  metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS tasks_project ON tasks(project);

CREATE TABLE IF NOT EXISTS attempts (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  generation INTEGER NOT NULL,
  session_id TEXT,
  directory TEXT NOT NULL,
  agent TEXT,
  model TEXT,
  variant TEXT,
  status TEXT NOT NULL,
  dispatch_state TEXT NOT NULL DEFAULT 'prepared',
  dispatch_msg_id TEXT,
  session_meta TEXT NOT NULL DEFAULT '{}',
  started_at INTEGER,
  dispatched_at INTEGER,
  last_progress_at INTEGER,
  last_observed_at INTEGER,
  deadline_at INTEGER,
  stall_after INTEGER,
  terminal_at INTEGER,
  terminal_kind TEXT,
  terminal_reason TEXT,
  stop_requested_at INTEGER,
  stop_confirmed_at INTEGER,
  site_released_at INTEGER,
  result_collected_at INTEGER,
  result_path TEXT,
  result_summary TEXT,
  verification TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  metadata TEXT NOT NULL DEFAULT '{}',
  UNIQUE(task_id, generation)
);
CREATE INDEX IF NOT EXISTS attempts_status ON attempts(status);
CREATE INDEX IF NOT EXISTS attempts_session ON attempts(session_id);
CREATE INDEX IF NOT EXISTS attempts_task ON attempts(task_id);

CREATE TABLE IF NOT EXISTS deliverables (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  attempt_id TEXT NOT NULL REFERENCES attempts(id),
  kind TEXT NOT NULL,
  target TEXT NOT NULL,
  required INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'expected',
  evidence TEXT,
  checked_at INTEGER,
  UNIQUE(attempt_id, kind, target)
);

CREATE TABLE IF NOT EXISTS turns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  attempt_id TEXT NOT NULL REFERENCES attempts(id),
  start_event_id TEXT,
  end_event_id TEXT,
  started_at INTEGER,
  ended_at INTEGER,
  outcome TEXT,
  UNIQUE(attempt_id, start_event_id)
);

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  session_id TEXT,
  task_id TEXT,
  attempt_id TEXT,
  seq INTEGER,
  version INTEGER,
  created_ms INTEGER,
  observed_at INTEGER NOT NULL,
  duplicate INTEGER NOT NULL DEFAULT 0,
  stale INTEGER NOT NULL DEFAULT 0,
  payload TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS events_session ON events(session_id);
CREATE INDEX IF NOT EXISTS events_attempt ON events(attempt_id);
CREATE INDEX IF NOT EXISTS events_type ON events(type);

CREATE TABLE IF NOT EXISTS notifications (
  id TEXT PRIMARY KEY,
  project TEXT,
  kind TEXT NOT NULL,
  target_session TEXT,
  task_id TEXT,
  attempt_id TEXT,
  dedupe_key TEXT NOT NULL,
  payload TEXT NOT NULL DEFAULT '{}',
  state TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  next_attempt_at INTEGER NOT NULL,
  message_id TEXT,
  created_at INTEGER NOT NULL,
  sent_at INTEGER,
  acked_at INTEGER,
  last_error TEXT,
  claim_owner TEXT,
  claim_at INTEGER,
  metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS notifications_due ON notifications(state, next_attempt_at);
CREATE UNIQUE INDEX IF NOT EXISTS notifications_active_dedupe
  ON notifications(dedupe_key) WHERE state IN ('pending','inflight','sent');

CREATE TABLE IF NOT EXISTS alerts (
  id TEXT PRIMARY KEY,
  severity TEXT NOT NULL,
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  detail TEXT NOT NULL,
  task_id TEXT,
  attempt_id TEXT,
  dedupe_key TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'new',
  created_at INTEGER NOT NULL,
  acked_at INTEGER,
  delivered_at INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS alerts_active_dedupe
  ON alerts(dedupe_key) WHERE state = 'new';

CREATE TABLE IF NOT EXISTS coordinator_leases (
  project TEXT PRIMARY KEY,
  holder_session TEXT NOT NULL,
  epoch INTEGER NOT NULL,
  acquired_at INTEGER NOT NULL,
  renewed_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  failure_count INTEGER NOT NULL DEFAULT 0,
  last_failure_at INTEGER,
  recovery_blocked INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at INTEGER NOT NULL,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  entity TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  from_state TEXT,
  to_state TEXT,
  detail TEXT
);
CREATE INDEX IF NOT EXISTS audit_entity ON audit(entity, entity_id);

""" + _SCHEMA_V2_DELTA + _SESSION_INPUTS_OUTSTANDING_INDEX + _SCHEMA_V3_DELTA

# 逐语句执行数组：新库初始化在同一 BEGIN IMMEDIATE 内原子完成
# （schema DDL + schema_version 写入），崩溃/并发不留半建表。
_SCHEMA_STATEMENTS = [s.strip() for s in SCHEMA.split(";") if s.strip()]


def db_path(home: str) -> str:
    return os.path.join(home, "ledger.db")


def connect(home: str) -> sqlite3.Connection:
    ensure_home(home)
    path = db_path(home)
    conn = sqlite3.connect(path, timeout=15, isolation_level=None)
    conn.row_factory = sqlite3.Row
    # 逻辑控制仍是schema3：显式发布门以DML trigger拒绝不认识新控制的旧
    # schema3写入者。先验证契约再执行可能改journal状态的PRAGMA；默认不安装。
    from .writer_guard import configure_writer_connection
    from .delivery_guard import configure_delivery_connection
    try:
        configure_writer_connection(conn)
        configure_delivery_connection(conn)
    except BaseException:
        conn.close()
        raise
    # busy_timeout 先于任何可能加锁的 PRAGMA，保证并发 init（WAL 切换）可等待。
    conn.execute("PRAGMA busy_timeout=15000")
    # journal_mode 已持久在文件；并发首个事务建表期间切 WAL 可能立刻 BUSY，
    # 有界重试（每次失败说明其他连接正持写锁，稍后即可）。
    for _ in range(10):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            break
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            time.sleep(0.02)
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _existing_version(conn: sqlite3.Connection) -> int | None:
    """读 meta.schema_version；无 meta 表返回 None（全新库）。只读、无 DDL。"""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='meta'"
    ).fetchone()
    if row is None:
        return None
    row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    if row is None:
        raise RuntimeError("台账 meta 表存在但缺少 schema_version；不支持的旧台账")
    try:
        return int(row["value"])
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"台账 schema_version 非法：{row['value']!r}") from exc


def _import_legacy_receipts_in_tx(conn: sqlite3.Connection) -> int:
    """旧 notifications.message_id 非空且未消费的通知 → unknown 历史回执。

    - 按 (target_session, message_id) **聚合**：共享同一物理输入的多条通知合并
      为完整通知集合（一个物理输入与多条通知关联，绝不只保留第一个成员）；
    - target_session 为空/缺失的旧回执**不迁移**（无目标可对账，不能造伪目标）；
    - 不判定 delivered、不批量重放（状态/attempts 不变）；outstanding=0 不占用
      未决槽位；返回真实导入的物理输入组数。
    """
    rows = conn.execute(
        "SELECT id, target_session, message_id, kind, created_at, sent_at "
        "FROM notifications WHERE message_id IS NOT NULL AND message_id<>'' "
        "AND target_session IS NOT NULL AND target_session<>'' "
        "AND state IN ('pending','sent','inflight')"
    ).fetchall()
    groups: dict[tuple[str, str], list] = {}
    for r in rows:
        groups.setdefault((r["target_session"], r["message_id"]), []).append(r)
    now = now_ms()
    for (session_id, message_id), members in groups.items():
        member_ids = sorted(m["id"] for m in members)
        kinds = sorted({m["kind"] for m in members})
        first = members[0]
        input_id = "L-" + sha256_text(f"{session_id}|{message_id}")[:20]
        base_at = min((m["created_at"] for m in members), default=now) or now
        sent_at = next((m["sent_at"] for m in members if m["sent_at"]), None)
        conn.execute(
            "INSERT OR IGNORE INTO session_inputs(input_id, session_id, message_id, "
            "source, intent, outstanding, revision, body, body_hash, "
            "notification_ids, admission_state, prepared_at, ack_deadline_at, "
            "observed_at, created_at, updated_at, metadata) "
            "VALUES(?,?,?,?,?,0,1,'','',?,?,?,?,?,?,?,?)",
            (input_id, session_id, message_id, "legacy-receipt", "unknown",
             json_dumps(member_ids), "unknown", base_at, sent_at, base_at,
             base_at, now,
             json_dumps({"legacy_receipt": True, "kinds": kinds,
                         "members": len(member_ids)})),
        )
    return len(groups)


def _apply_1_to_2_in_tx(conn: sqlite3.Connection) -> int:
    """在**已开启的事务内**完成 1→2：表、普通索引、历史回执、唯一索引、版本。

    返回真实导入的历史回执组数。刻意不自己开事务，供 1→2→3 链式升级复用。
    """
    conn.execute("SELECT 1 FROM tasks LIMIT 1")  # 确认 v1 表可读
    conn.execute(_SESSION_INPUTS_TABLE)
    for stmt in _SESSION_INPUTS_BASE_INDEXES:
        conn.execute(stmt)
    imported = _import_legacy_receipts_in_tx(conn)
    conn.execute(_SESSION_INPUTS_OUTSTANDING_INDEX)
    conn.execute(
        "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        ("2",),
    )
    conn.execute(
        "INSERT INTO audit(at, actor, action, entity, entity_id, "
        "from_state, to_state, detail) VALUES(?,?,?,?,?,?,?,?)",
        (now_ms(), "migration", "schema.migrate", "schema", "1", "1", "2",
         json_dumps({"schema_version": 2, "legacy_receipts": imported})),
    )
    return imported


def _apply_2_to_3_in_tx(conn: sqlite3.Connection,
                        migrate_hook: Callable[[sqlite3.Connection], None] | None
                        ) -> None:
    """在**已开启的事务内**完成 2→3：responsibilities 表/索引/唯一约束/版本。

    - 只做结构与版本，**不回填任何责任行**：历史工作缺可核验归属，迁移不造
      “已执行/已处理”事实，也不重播历史消息；
    - ``migrate_hook`` 在版本号写入前调用（历史语义：事务内注入失败 → 整体回滚）。
    """
    conn.execute("SELECT 1 FROM session_inputs LIMIT 1")  # 确认 v2 表可读
    conn.execute(_RESPONSIBILITIES_TABLE)
    for stmt in _RESPONSIBILITIES_INDEXES:
        conn.execute(stmt)
    conn.execute(_RESPONSIBILITIES_CONDITION_INDEX)
    if migrate_hook is not None:
        # 仅供迁移演练注入失败；事务内任何异常 → ROLLBACK。
        migrate_hook(conn)
    conn.execute(
        "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        ("3",),
    )
    conn.execute(
        "INSERT INTO audit(at, actor, action, entity, entity_id, "
        "from_state, to_state, detail) VALUES(?,?,?,?,?,?,?,?)",
        (now_ms(), "migration", "schema.migrate", "schema", "2", "2", "3",
         json_dumps({"schema_version": 3, "responsibilities": 0,
                     "backfilled": False})),
    )


def _migrate_1_to_3(conn: sqlite3.Connection,
                    migrate_hook: Callable[[sqlite3.Connection], None] | None
                    ) -> None:
    """v1 → v3 链式升级：两步同一 BEGIN IMMEDIATE，失败整体回到 v1。"""
    with transaction(conn):
        _apply_1_to_2_in_tx(conn)
        _apply_2_to_3_in_tx(conn, migrate_hook)


def _migrate_2_to_3(conn: sqlite3.Connection,
                    migrate_hook: Callable[[sqlite3.Connection], None] | None
                    ) -> None:
    """v2 → v3 短事务升级：只加 responsibilities 结构，不改既有行。"""
    with transaction(conn):
        _apply_2_to_3_in_tx(conn, migrate_hook)


def init(home: str, *, expect_version: int | None = None,
         migrate_hook: Callable[[sqlite3.Connection], None] | None = None,
         fresh_hook: Callable[[sqlite3.Connection], None] | None = None
         ) -> sqlite3.Connection:
    """初始化/打开台账（先版本检查再建 schema）。

    - 全新目录：schema DDL 与 schema_version 写入在同一 BEGIN IMMEDIATE
      内**原子**完成（崩溃/并发不留半建表；两连接并发生成同一空库幂等），
      失败整体回滚。
    - 已存在且版本 == 期望：版本一致时幂等确保表结构（IF NOT EXISTS）。
    - 既有 v1：在同一事务内链式 1→2→3（失败整体回滚到 v1）。
    - 既有 v2：执行显式 2→3 迁移（只加 responsibilities，失败整体回滚）。
    - 其余（future schema / expect_version 过旧或未来）：RuntimeError；关闭
      连接、无任何 DDL、不改动已有用户数据。

    expect_version / migrate_hook / fresh_hook 仅供迁移演练：expect_version
    模拟旧二进制的目标版本（旧版本打开新 schema 必须拒绝）；migrate_hook 在
    迁移事务内、版本写入前调用，失败即回滚；fresh_hook 在新库事务内注入失败
    以验证原子性。生产调用保持 init(home)。
    """
    wanted = SCHEMA_VERSION if expect_version is None else int(expect_version)
    conn = connect(home)
    try:
        existing = _existing_version(conn)
        if existing is None:
            # 全新库：只允许按当前版本初始化。DDL + 版本号同一事务（原子、幂等）。
            if wanted != SCHEMA_VERSION:
                raise RuntimeError(
                    f"台账 schema 版本不兼容：空库不能以 v{wanted} 打开"
                    f"（当前 v{SCHEMA_VERSION}）")
            with transaction(conn):
                for stmt in _SCHEMA_STATEMENTS:
                    conn.execute(stmt)
                if fresh_hook is not None:
                    # 仅供演练注入失败验证原子性；异常 → 整体回滚（无半建表）。
                    fresh_hook(conn)
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(SCHEMA_VERSION),),
                )
            return conn
        if existing == wanted:
            # 版本一致：幂等确保（不会先建 future schema 再拒绝）。
            if wanted == SCHEMA_VERSION:
                conn.executescript(SCHEMA)
            return conn
        if wanted == SCHEMA_VERSION and existing == 1:
            if SCHEMA_VERSION != 3:
                raise RuntimeError(
                    f"当前代码 SCHEMA_VERSION={SCHEMA_VERSION} 不支持 1→2→3 迁移")
            _migrate_1_to_3(conn, migrate_hook=migrate_hook)
            return conn
        if wanted == SCHEMA_VERSION and existing == 2:
            if SCHEMA_VERSION != 3:
                raise RuntimeError(
                    f"当前代码 SCHEMA_VERSION={SCHEMA_VERSION} 不支持 2→3 迁移")
            _migrate_2_to_3(conn, migrate_hook=migrate_hook)
            return conn
        raise RuntimeError(f"台账 schema 版本不兼容：{existing} != {wanted}")
    except BaseException:
        conn.close()
        raise


@contextlib.contextmanager
def read_snapshot(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """跨表只读投影共用一个WAL快照，不占写锁、不推进游标。"""
    if conn.in_transaction:
        yield conn
        return
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        conn.execute("ROLLBACK")


@contextlib.contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """显式 BEGIN IMMEDIATE；异常回滚。不可嵌套。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


# ---- 一致备份（可恢复；JSON export 不是备份） -------------------------
def backup_to(conn: sqlite3.Connection, dest_path: str) -> str:
    """用 SQLite backup API 生成**一致、可恢复**的备份（含 WAL 未 checkpoint）。

    备份包含 schema3 的全部事实：session_inputs（输入回执/原 message 身份）、
    responsibilities（责任/预算/期限/转交链）、notifications（含 ACK/deadline）、
    meta（schema_version/target_gate）、tasks/attempts/events 等。写入临时名后
    ``os.replace`` 原子落地，并把日志模式转为 DELETE，使备份成为单文件、可只读
    打开。**JSON export 不满足恢复要求**（无索引/事务事实）。
    """
    directory = os.path.dirname(os.path.abspath(dest_path)) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = f"{dest_path}.tmp-{os.getpid()}"
    if os.path.exists(tmp):
        os.unlink(tmp)
    dest = sqlite3.connect(tmp, isolation_level=None)
    try:
        conn.backup(dest)
        dest.execute("PRAGMA journal_mode=DELETE")
        dest.execute("PRAGMA synchronous=FULL")
    finally:
        dest.close()
    os.replace(tmp, dest_path)
    return dest_path


def inspect_backup(path: str) -> dict:
    """**只读**核验备份：integrity_check、schema_version 与关键事实表计数。

    以 ``mode=ro`` 打开（不会创建/修改备份，也不产生 -wal 写入）；返回
    ``{path, integrity, schema_version, tables:{table: count}, ok}``。
    """
    import os as _os
    uri = "file:" + _os.path.abspath(path) + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        version = None
        tables: dict[str, int] = {}
        names = [r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "meta" in names:
            row = conn.execute(
                "SELECT value FROM meta WHERE key='schema_version'").fetchone()
            version = row["value"] if row else None
        for table in ("tasks", "attempts", "notifications", "session_inputs",
                      "responsibilities", "alerts", "events", "audit"):
            if table in names:
                tables[table] = int(conn.execute(
                    f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        return {"path": path, "integrity": integrity, "schema_version": version,
                "tables": tables,
                "ok": integrity == "ok" and version is not None}
    finally:
        conn.close()


def meta_get(conn: sqlite3.Connection, key: str, default=None):
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )

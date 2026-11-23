"""S4 B 组存储层：ManagedInputLedger（受管输入回执 + 显式白名单观察）。

只读本地 SQLite；**无任何外部 I/O**：不构造 Client、不请求模型、不伪造
Task/Attempt。复用 ``session_inputs``（source='managed_input'，outstanding=0）与
版本化 ``meta``，不新增 DDL、不改 schema 版本；P0 自动 wake 的部分唯一索引与
方法保持不变。

契约详见 `evidence/2026-11-09-managed-input-development/B-api.md`。要点：

- 独立输入以 ``request_id`` 在 session 作用域持久幂等：同 session + 同 request_id
  必须正文/intent/owner/project（及 park 的 expires_at/flush_condition）全相同，
  否则拒绝；绝不与 P0 自动 wake 合并。
- 操作 claim（send/flush/cancel）为 owner + expected_revision CAS 一次赢家：只有
  changed 赢家可执行外部调用；reason 持久；未知结果沿原 message_id / 原输入 ID /
  operation_revision 延续，不能借同 body 新 ID 重放。
- 证据（admission/delivery/cancelled/unknown）单调：重复不计 send、不重置时间；
  delivery 与 cancel 冲突不伪造 ACK；execution/business_ack 只留观察事实，绝不
  覆盖执行证据或业务 ACK。
- park 元数据（owner/flush_condition/expires_at）必须齐且 expiry_policy 仅 alert；
  到期只告警（持久 ``expiry_alert`` 标记 + condition_key，ACK 后同一条件不再
  再造告警），不 cancel/flush/改已 ACK/不烧 budget。
- 白名单默认空：owner+project+max_pending_age_ms 显式登记，撤销 CAS，撤销后晚到
  观察不重新启用；全读写有界 limit、fail-closed 校验，不扫描无关 meta。
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from ..domain.errors import LedgerError
from ..util import json_dumps, json_loads, sha256_text, short_id
from .input_ledger import (
    ADMISSION_ADMITTED,
    ADMISSION_CANCELLED,
    ADMISSION_DELIVERED,
    ADMISSION_EXPIRED,
    ADMISSION_POSSIBLE,
    ADMISSION_PREPARED,
    ADMISSION_RELEASED,
    ADMISSION_UNKNOWN,
    INTENT_EXECUTE,
    INTENT_PARK,
    INTENT_RECORD,
)
from .sqlite_db import meta_get, meta_set, transaction

SOURCE_MANAGED_INPUT = "managed_input"
# 独立留档终态：record 无发送阶段。
ADMISSION_RECORDED = "recorded"

# 受管输入元数据独立 schema 版本（仅 meta/metadata 内，不升级 DDL）。
MANAGED_META_SCHEMA = 1
WATCH_SCHEMA = 1
WATCH_KEY_PREFIX = "managed-watch:"

OP_SEND = "send"
OP_FLUSH = "flush"
OP_CANCEL = "cancel"
ALLOWED_OPERATIONS = frozenset({OP_SEND, OP_FLUSH, OP_CANCEL})

_INTENTS = frozenset({INTENT_EXECUTE, INTENT_PARK, INTENT_RECORD})
_TERMINAL = frozenset({
    ADMISSION_CANCELLED, ADMISSION_DELIVERED, ADMISSION_EXPIRED,
    ADMISSION_RELEASED, ADMISSION_RECORDED,
})
# send claim 允许状态；admitted 已受理拒重发；delivered/终态拒。
_SEND_FROM = frozenset({ADMISSION_PREPARED, ADMISSION_POSSIBLE, ADMISSION_UNKNOWN})
# flush claim 允许状态（需要 inbox 项存在：已发送且尚未投递/未知）。
_FLUSH_FROM = frozenset({ADMISSION_POSSIBLE, ADMISSION_UNKNOWN, ADMISSION_ADMITTED})
# cancel claim 允许状态；delivered 后 DELETE 204/no-op ≠ 取消成功。
_CANCEL_FROM = frozenset({ADMISSION_PREPARED, ADMISSION_POSSIBLE,
                          ADMISSION_UNKNOWN, ADMISSION_ADMITTED})

# 有效时钟边界（毫秒）：拒绝荒谬时间（与 target_gate.MAX_CLOCK_MS 同量级）。
MAX_CLOCK_MS = 8 * 10 ** 12
# 白名单最近观察有界数量；超限丢弃最旧。
WATCH_MAX_OBSERVATIONS = 16
# 列表/扫描上限（limit 界内）。
DEFAULT_LIMIT_MAX = 500

_WATCH_REQUIRED = (
    ("session_id", "str"),
    ("owner", "str"),
    ("project", "str"),
    ("max_pending_age_ms", "int"),
    ("enabled", "bool"),
    ("revision", "int"),
    ("registered_at", "int"),
    ("updated_at", "int"),
)
_EVIDENCE_KINDS = ("admitted", "delivered", "cancelled", "unknown",
                   "execution", "business_ack")
_TRACKING_STR_KEYS = (
    "task_id", "attempt_id", "result_version", "work_kind", "target_session",
    "responsibility_id", "wait_condition", "boundary", "deadline_source",
    "materials_source",
)
_TRACKING_INT_KEYS = ("generation", "responsibility_revision", "next_check_at",
                      "deadline")


# SQL 侧等价的条件指纹表达式（与 _park_condition_key 逐字一致），用于在
# LIMIT 之前过滤“同一未变条件已告警”的行，避免已标前缀长期占用候选窗。
_PARK_CK_SQL = (
    "'park-expired:v1:' || json_extract(metadata,'$.managed_input.expires_at')"
    " || ':' || json_extract(metadata,'$.managed_input.flush_condition')"
    " || ':' || substr(body_hash,1,16)"
)


# ---- 校验辅助 ------------------------------------------------------------
def _is_str(v) -> bool:
    return isinstance(v, str)


def _non_empty_str(v) -> bool:
    return isinstance(v, str) and bool(v.strip())


def _is_int_not_bool(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _valid_ms(value: Any) -> int:
    """now 必须为界内非负纯整数；否则 INVALID_TIMESTAMP。"""
    if not _is_int_not_bool(value) or not (0 <= value <= MAX_CLOCK_MS):
        raise LedgerError(
            "INVALID_TIMESTAMP",
            f"now 必须为有效时钟（0..{MAX_CLOCK_MS}）内的整数（收到 {value!r}）")
    return value


def _bounded_limit(limit: Any, default: int = 100) -> int:
    if limit is None:
        return default
    if not _is_int_not_bool(limit) or limit < 1:
        raise LedgerError("INVALID_INPUT", f"limit 必须为 >=1 的整数（收到 {limit!r}）")
    return min(int(limit), DEFAULT_LIMIT_MAX)


def _managed_block(meta: dict, input_id: str) -> dict:
    """提取并严格校验受管输入元数据块；坏/未来 schema 一律 fail-closed。"""
    if not isinstance(meta, dict):
        raise LedgerError("MANAGED_META_CORRUPT",
                          f"输入 {input_id} metadata 非 dict，不能作为受管输入")
    block = meta.get("managed_input")
    if not isinstance(block, dict):
        raise LedgerError(
            "MANAGED_META_CORRUPT",
            f"输入 {input_id} 缺少 managed_input 元数据块（不受管/损坏）")
    if block.get("schema") != MANAGED_META_SCHEMA:
        raise LedgerError(
            "MANAGED_META_CORRUPT",
            f"输入 {input_id} managed_input schema={block.get('schema')!r} "
            f"!= {MANAGED_META_SCHEMA}")
    for key in ("request_id", "project"):
        if not _non_empty_str(block.get(key)) and key == "request_id":
            raise LedgerError("MANAGED_META_CORRUPT",
                              f"输入 {input_id} managed_input.{key} 缺非空 str")
    for key, kind in (("expires_at", "int"), ("recorded_at", "int")):
        v = block.get(key)
        if v is not None and not _is_int_not_bool(v):
            raise LedgerError("MANAGED_META_CORRUPT",
                              f"输入 {input_id} managed_input.{key} 非 int/None")
    fc = block.get("flush_condition")
    if fc is not None and not isinstance(fc, str):
        raise LedgerError("MANAGED_META_CORRUPT",
                          f"输入 {input_id} managed_input.flush_condition 非 str/None")
    ep = block.get("expiry_policy")
    if ep is not None and ep != "alert":
        raise LedgerError("MANAGED_META_CORRUPT",
                          f"输入 {input_id} expiry_policy 仅允许 alert（收到 {ep!r}）")
    resume = block.get("resume")
    if resume is not None and not isinstance(resume, bool):
        raise LedgerError("MANAGED_META_CORRUPT",
                          f"输入 {input_id} managed_input.resume 非 bool/None")
    tracking = block.get("tracking")
    if tracking is not None:
        if not isinstance(tracking, dict):
            raise LedgerError("MANAGED_META_CORRUPT",
                              f"输入 {input_id} managed_input.tracking 非 dict/None")
        if "tracked" in tracking and not isinstance(tracking["tracked"], bool):
            raise LedgerError("MANAGED_META_CORRUPT",
                              f"输入 {input_id} tracking.tracked 非 bool")
        for key in _TRACKING_STR_KEYS:
            value = tracking.get(key)
            if value is not None and not isinstance(value, str):
                raise LedgerError(
                    "MANAGED_META_CORRUPT",
                    f"输入 {input_id} tracking.{key} 非 str/None")
        for key in _TRACKING_INT_KEYS:
            value = tracking.get(key)
            if value is not None and not _is_int_not_bool(value):
                raise LedgerError(
                    "MANAGED_META_CORRUPT",
                    f"输入 {input_id} tracking.{key} 非 int/None")
        materials = tracking.get("materials")
        if materials is not None and not isinstance(materials, list):
            raise LedgerError("MANAGED_META_CORRUPT",
                              f"输入 {input_id} tracking.materials 非 list/None")
        sources = tracking.get("attribution_sources")
        if sources is not None and not isinstance(sources, dict):
            raise LedgerError("MANAGED_META_CORRUPT",
                              f"输入 {input_id} tracking.attribution_sources 非 dict/None")
    for sub in ("operations", "evidence"):
        if not isinstance(block.get(sub), dict):
            raise LedgerError("MANAGED_META_CORRUPT",
                              f"输入 {input_id} managed_input.{sub} 非 dict")
    al = block.get("expiry_alert")
    if al is not None and not isinstance(al, dict):
        raise LedgerError("MANAGED_META_CORRUPT",
                          f"输入 {input_id} managed_input.expiry_alert 非 dict/None")
    lc = block.get("last_checked_at")
    if lc is not None and not _is_int_not_bool(lc):
        raise LedgerError("MANAGED_META_CORRUPT",
                          f"输入 {input_id} managed_input.last_checked_at 非 int/None")
    flush = block.get("flush")
    if flush is not None:
        if not isinstance(flush, dict):
            raise LedgerError("MANAGED_META_CORRUPT",
                              f"输入 {input_id} managed_input.flush 非 dict/None")
        if not _is_int_not_bool(flush.get("flushed_at")):
            raise LedgerError("MANAGED_META_CORRUPT",
                              f"输入 {input_id} flush.flushed_at 非 int")
        if flush.get("effective_intent") not in _INTENTS:
            raise LedgerError("MANAGED_META_CORRUPT",
                              f"输入 {input_id} flush.effective_intent 非法")
        if not isinstance(flush.get("effective_resume"), bool):
            raise LedgerError("MANAGED_META_CORRUPT",
                              f"输入 {input_id} flush.effective_resume 非 bool")
    return block


def _input_from_row(row: sqlite3.Row, *, with_body: bool = True) -> dict:
    d = dict(row)
    d["notification_ids"] = json_loads(d.get("notification_ids"), [])
    d["outstanding"] = bool(d["outstanding"])
    d["metadata"] = json_loads(d.get("metadata"), {})
    # managed 行：严格校验元数据（坏/未来 schema 拒绝）；automatic_wake/legacy 不校验。
    if d["source"] == SOURCE_MANAGED_INPUT:
        _managed_block(d["metadata"], d["input_id"])
    if not with_body:
        d.pop("body", None)
    return d


def _tracking_identity(tracking: Any) -> tuple:
    """用于 request_id 幂等的稳定归属指纹；不含时间默认/责任绑定等易变字段。

    显式 deadline/materials 必须参与冲突判定；默认值重试保持幂等。
    """
    if not isinstance(tracking, dict):
        return ()
    materials = tracking.get("materials")
    if isinstance(materials, list):
        materials = tuple(sorted(json.dumps(m, ensure_ascii=False, sort_keys=True)
                                 if isinstance(m, dict) else str(m)
                                 for m in materials))
    else:
        materials = ()
    deadline_key = None
    if tracking.get("deadline_source") == "explicit":
        deadline_key = tracking.get("deadline")
    materials_key = None
    if tracking.get("materials_source") == "explicit":
        materials_key = materials
    return (
        bool(tracking.get("tracked")),
        tracking.get("task_id"), tracking.get("attempt_id"),
        tracking.get("generation"), tracking.get("result_version"),
        tracking.get("work_kind"), tracking.get("target_session"),
        tracking.get("boundary"), deadline_key, materials_key,
    )


class ManagedInputLedger:
    """受管输入回执 + 显式白名单观察（S4 B 组存储窄端口）。"""

    def __init__(self, conn: sqlite3.Connection, base_ledger: Any):
        self.conn = conn
        self.base = base_ledger
        self._now_ms = base_ledger._now_ms  # type: ignore  # 复用注入时钟

    # ---- 只读 -----------------------------------------------------
    def get_managed_input(self, input_id: str) -> dict | None:
        if not _non_empty_str(input_id):
            raise LedgerError("INVALID_INPUT", "input_id 必须为非空字符串")
        row = self.conn.execute(
            "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
        ).fetchone()
        return _input_from_row(row) if row is not None else None

    def list_managed_inputs(self, *, session_id: str | None = None,
                            limit: int = 100) -> list[dict]:
        lim = _bounded_limit(limit)
        sql = "SELECT * FROM session_inputs WHERE source=?"
        args: list[Any] = [SOURCE_MANAGED_INPUT]
        if session_id:
            sql += " AND session_id=?"
            args.append(session_id)
        sql += " ORDER BY created_at ASC, input_id ASC LIMIT ?"
        args.append(lim)
        rows = self.conn.execute(sql, args).fetchall()
        # 列表默认不含正文。
        return [_input_from_row(r, with_body=False) for r in rows]

    def list_pending_managed_inputs_page(
        self, *, after_id: str | None = None, limit: int = 100,
    ) -> list[dict]:
        """按 input_id 升序的有界待对账页（source=managed_input，非 record）。

        只包含尚未确认 delivered/cancelled/released 的行（prepared/possible/
        unknown/admitted/expired 等）；不扫全局 session，不做网络调用。
        C 可持久 ``next_after_id`` 作为游标，按原 ID 安全 reconcile。
        """
        lim = _bounded_limit(limit)
        if after_id is not None and not _non_empty_str(after_id):
            raise LedgerError("INVALID_INPUT", "after_id 必须是非空字符串/None")
        sql = ("SELECT * FROM session_inputs WHERE source=? "
               "AND admission_state NOT IN "
               "('delivered','cancelled','released','recorded') ")
        args: list[Any] = [SOURCE_MANAGED_INPUT]
        if after_id is not None:
            sql += "AND input_id > ? "
            args.append(after_id)
        sql += "ORDER BY input_id ASC LIMIT ?"
        args.append(lim)
        rows = self.conn.execute(sql, args).fetchall()
        return [_input_from_row(r, with_body=True) for r in rows]

    def list_legacy_inputs(self, *, session_id: str | None = None,
                           limit: int = 100) -> list[dict]:
        """历史 legacy-receipt 只读列表；不猜测 delivered、不重放。"""
        lim = _bounded_limit(limit)
        sql = "SELECT * FROM session_inputs WHERE source='legacy-receipt'"
        args: list[Any] = []
        if session_id:
            sql += " AND session_id=?"
            args.append(session_id)
        sql += " ORDER BY created_at ASC, input_id ASC LIMIT ?"
        args.append(lim)
        rows = self.conn.execute(sql, args).fetchall()
        return [_input_from_row(r, with_body=False) for r in rows]

    # ---- 准备/幂等 -------------------------------------------------
    def prepare_managed_input(
        self, session_id: str, *,
        intent: str, text: str, owner: str, request_id: str,
        project: str = "", expires_at: int | None = None,
        flush_condition: str | None = None, now: int,
        message_id: str | None = None, tracking: dict | None = None,
    ) -> dict:
        if not _non_empty_str(session_id):
            raise LedgerError("INVALID_INPUT", "目标 session 不能为空")
        if intent not in _INTENTS:
            raise LedgerError("INVALID_INTENT",
                              f"intent 必须是 execute/park/record（收到 {intent!r}）")
        if not _non_empty_str(text):
            raise LedgerError("INVALID_INPUT", "正文不能为空")
        if not _non_empty_str(owner):
            raise LedgerError("INVALID_OWNER", "owner 必须为非空字符串")
        if not _non_empty_str(request_id):
            raise LedgerError("INVALID_INPUT", "request_id 必须为非空字符串")
        if project is not None and not isinstance(project, str):
            raise LedgerError("INVALID_INPUT", "project 必须为 str")
        if message_id is not None and not _non_empty_str(message_id):
            raise LedgerError("INVALID_INPUT", "message_id 必须为非空字符串/None")
        if tracking is not None and not isinstance(tracking, dict):
            raise LedgerError("INVALID_INPUT", "tracking 必须为 dict/None")
        ts = _valid_ms(now)

        meta_block: dict[str, Any] = {
            "schema": MANAGED_META_SCHEMA,
            "request_id": request_id,
            "project": project or "",
        }
        if tracking is not None:
            # 只保存引用/归属元数据；正文与材料内容仍由调用方自持。
            meta_block["tracking"] = dict(tracking)
        if intent == INTENT_EXECUTE:
            meta_block["resume"] = True
            meta_block["expires_at"] = None
            meta_block["flush_condition"] = None
            meta_block["expiry_policy"] = None
        elif intent == INTENT_PARK:
            if not _is_int_not_bool(expires_at) or expires_at < 0:
                raise LedgerError(
                    "INVALID_INPUT", "park 必须提供非负 int expires_at")
            if not _non_empty_str(flush_condition):
                raise LedgerError(
                    "INVALID_INPUT", "park 必须提供非空 flush_condition")
            meta_block["resume"] = False
            meta_block["expires_at"] = int(expires_at)
            meta_block["flush_condition"] = flush_condition
            meta_block["expiry_policy"] = "alert"
        else:  # record：无发送阶段
            meta_block["resume"] = None
            meta_block["expires_at"] = None
            meta_block["flush_condition"] = None
            meta_block["expiry_policy"] = None
        meta_block["recorded_at"] = ts if intent == INTENT_RECORD else None
        meta_block["expiry_alert"] = None
        meta_block["operations"] = {OP_SEND: None, OP_FLUSH: None, OP_CANCEL: None}
        meta_block["evidence"] = {k: None for k in _EVIDENCE_KINDS}

        with transaction(self.conn):
            # request_id 幂等（session 作用域，事务内串行）。
            cur = self.conn.execute(
                "SELECT * FROM session_inputs WHERE session_id=? AND source=? "
                "ORDER BY created_at ASC, input_id ASC",
                (session_id, SOURCE_MANAGED_INPUT),
            ).fetchall()
            for row in cur:
                d = _input_from_row(row)
                block = d["metadata"]["managed_input"]
                if block["request_id"] != request_id:
                    continue
                same = (d["intent"] == intent and d["owner"] == owner
                        and d["body"] == text
                        and block.get("project", "") == (project or "")
                        and block.get("expires_at") == meta_block["expires_at"]
                        and block.get("flush_condition")
                        == meta_block["flush_condition"]
                        and (message_id is None or d["message_id"] == message_id)
                        and _tracking_identity(block.get("tracking"))
                        == _tracking_identity(tracking))
                if same:
                    return {"input": d, "created": False}
                raise LedgerError(
                    "REQUEST_ID_CONFLICT",
                    f"session {session_id} 已存在 request_id={request_id}，但"
                    "正文/intent/owner/project、message_id 或归属引用不同，拒绝复用",
                    {"input_id": d["input_id"]})
            if message_id is not None:
                reused = self.conn.execute(
                    "SELECT input_id FROM session_inputs "
                    "WHERE session_id=? AND message_id=? LIMIT 1",
                    (session_id, message_id),
                ).fetchone()
                if reused is not None:
                    raise LedgerError(
                        "INPUT_MESSAGE_ID_CONFLICT",
                        f"session {session_id} 已用原 message_id={message_id} "
                        "记录其他输入；不换身份复用/重投",
                        {"input_id": reused["input_id"]})
            # 独立受管行：outstanding=0，与自动 wake 槽（outstanding=1 部分唯一索引）隔离。
            input_id = short_id("MI-", 12)
            candidate_message_id = message_id or ("msg_ocorch" + short_id("", 18))
            state = ADMISSION_RECORDED if intent == INTENT_RECORD else ADMISSION_PREPARED
            body_hash = sha256_text(text)
            meta = {"managed_input": meta_block}
            self.conn.execute(
                "INSERT INTO session_inputs(input_id, session_id, message_id, source, "
                "intent, outstanding, revision, owner, body, body_hash, "
                "notification_ids, admission_state, prepared_at, created_at, "
                "updated_at, metadata) "
                "VALUES(?,?,?,?,?,0,1,?,?,?,'[]',?,?,?,?,?)",
                (input_id, session_id, candidate_message_id, SOURCE_MANAGED_INPUT,
                 intent, owner, text, body_hash, state, ts, ts, ts,
                 json_dumps(meta)),
            )
            self.base._audit(self.conn, owner, "managed.prepare", "session_input",
                             input_id, None, state,
                             {"intent": intent, "request_id": request_id,
                              "session": session_id,
                              "message_id": candidate_message_id,
                              "owner": owner, "project": project or ""})
            fresh = _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone())
            return {"input": fresh, "created": True}

    # ---- 绑定后续责任（发送前/证据更新；不改变输入业务状态） ------------
    def bind_managed_responsibility(
        self, input_id: str, *, responsibility_id: str,
        responsibility_revision: int, now: int,
        wait_condition: str | None = None, next_check_at: int | None = None,
        deadline: int | None = None, actor: str = "system",
        touch_revision: bool = False,
    ) -> dict:
        """把后续责任身份写回受管输入 tracking（短事务，不伪造发送事实）。

        - 只允许 ``source='managed_input'``；责任 ID 非空、revision 正整数。
        - 默认不递增输入 revision（发送前 claim 仍按当前 revision CAS）；
          ``touch_revision=True`` 时显式递增，供调用方要求 CAS 可见性。
        - 只更新 tracking 与 updated_at，不改 admission_state/正文/操作 claim。
        """
        if not _non_empty_str(input_id):
            raise LedgerError("INVALID_INPUT", "input_id 必须为非空字符串")
        if not _non_empty_str(responsibility_id):
            raise LedgerError("INVALID_INPUT",
                              "responsibility_id 必须为非空字符串")
        if not _is_int_not_bool(responsibility_revision) \
                or responsibility_revision < 1:
            raise LedgerError("INVALID_INPUT",
                              "responsibility_revision 必须为正整数")
        if wait_condition is not None and not isinstance(wait_condition, str):
            raise LedgerError("INVALID_INPUT", "wait_condition 必须为 str/None")
        for name, value in (("next_check_at", next_check_at),
                            ("deadline", deadline)):
            if value is not None and not _is_int_not_bool(value):
                raise LedgerError("INVALID_INPUT", f"{name} 必须为 int/None")
        ts = _valid_ms(now)
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_managed(d)
            block = dict(d["metadata"]["managed_input"])
            tracking = dict(block.get("tracking") or {})
            tracking.update({
                "responsibility_id": responsibility_id,
                "responsibility_revision": int(responsibility_revision),
            })
            if wait_condition is not None:
                tracking["wait_condition"] = wait_condition
            if next_check_at is not None:
                tracking["next_check_at"] = int(next_check_at)
            if deadline is not None:
                tracking["deadline"] = int(deadline)
            block["tracking"] = tracking
            revision = d["revision"] + (1 if touch_revision else 0)
            self.conn.execute(
                "UPDATE session_inputs SET revision=?, metadata=?, updated_at=? "
                "WHERE input_id=?",
                (revision, json_dumps(dict(d["metadata"], managed_input=block)),
                 ts, input_id),
            )
            self.base._audit(
                self.conn, actor, "managed.bind_responsibility", "session_input",
                input_id, d["admission_state"], d["admission_state"],
                {"responsibility_id": responsibility_id,
                 "responsibility_revision": int(responsibility_revision),
                 "touch_revision": bool(touch_revision)})
            updated = _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone())
            return {"input": updated, "revision": revision}

    # ---- park flush 成功事实（原 intent 保留，新增 effective 执行状态） ----
    def mark_managed_flushed(
        self, input_id: str, *, now: int, flush_operation_revision: int,
        effective_intent: str = "execute", effective_resume: bool = True,
        actor: str = "system",
    ) -> dict:
        """PATCH steer 成功后落 flushed_at/effective intent/resume。

        只接受对应 flush claim 的 operation_revision；unknown/PATCH 失败不得调用。
        原 ``intent`` 列保持不变（park 历史可追溯），metadata.managed_input.flush
        记录 effective 执行状态。到期扫描按 flushed_at 排除，不再 park-expired。
        """
        if not _is_int_not_bool(flush_operation_revision):
            raise LedgerError("INVALID_INPUT",
                              "flush_operation_revision 必须为整数")
        if effective_intent not in (INTENT_EXECUTE, INTENT_PARK, INTENT_RECORD):
            raise LedgerError("INVALID_INTENT",
                              f"effective_intent 非法：{effective_intent!r}")
        if not isinstance(effective_resume, bool):
            raise LedgerError("INVALID_INPUT", "effective_resume 必须为 bool")
        ts = _valid_ms(now)
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_managed(d)
            block = d["metadata"]["managed_input"]
            ops = block.get("operations") or {}
            claim = ops.get(OP_FLUSH)
            if not isinstance(claim, dict) or \
                    claim.get("operation_revision") != flush_operation_revision:
                raise LedgerError(
                    "STALE_OBSERVATION",
                    "flush 成功标记必须绑定当前 flush claim revision；"
                    "unknown/旧操作不得冒充成功",
                    {"stored": (claim or {}).get("operation_revision")})
            block = dict(block)
            block["flush"] = {
                "flushed_at": ts,
                "effective_intent": effective_intent,
                "effective_resume": bool(effective_resume),
                "operation_revision": flush_operation_revision,
                "actor": actor,
            }
            new_meta = dict(d["metadata"])
            new_meta["managed_input"] = block
            revision = d["revision"] + 1
            self.conn.execute(
                "UPDATE session_inputs SET revision=?, metadata=?, "
                "observed_at=?, updated_at=? WHERE input_id=?",
                (revision, json_dumps(new_meta), ts, ts, input_id),
            )
            self.base._audit(
                self.conn, actor, "managed.flush", "session_input", input_id,
                d["admission_state"], d["admission_state"],
                {"flushed_at": ts, "effective_intent": effective_intent,
                 "effective_resume": bool(effective_resume),
                 "operation_revision": flush_operation_revision})
            updated = _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone())
            return {"input": updated, "revision": revision,
                    "flushed_at": ts}
    def claim_managed_operation(
        self, input_id: str, *,
        operation: str, owner: str, expected_revision: int, reason: str, now: int,
    ) -> dict:
        if operation not in ALLOWED_OPERATIONS:
            raise LedgerError("INVALID_OPERATION",
                              f"operation 必须是 send/flush/cancel（收到 {operation!r}）")
        if not _non_empty_str(owner):
            raise LedgerError("INVALID_OWNER", "claim 必须提供非空 owner")
        if not _non_empty_str(reason):
            raise LedgerError("INVALID_INPUT", "操作必须有非空原因 reason")
        if not _is_int_not_bool(expected_revision):
            raise LedgerError(
                "INPUT_REVISION_CONFLICT",
                f"expected_revision 必须为整数（收到 {expected_revision!r}）")
        ts = _valid_ms(now)
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_managed(d)
            if d["owner"] != owner:
                raise LedgerError(
                    "INPUT_OWNER_MISMATCH",
                    f"输入 {input_id} 由 {d['owner']} 持有，{owner} 无操作权")
            if d["revision"] != expected_revision:
                raise LedgerError(
                    "INPUT_REVISION_CONFLICT",
                    f"输入 {input_id} revision={d['revision']} != 期望 "
                    f"{expected_revision}；并发修改或过期快照")
            if d["admission_state"] in _TERMINAL:
                raise LedgerError(
                    "INPUT_TERMINAL",
                    f"输入 {input_id} 已处于终态 {d['admission_state']}，不能 claim")
            block = d["metadata"]["managed_input"]
            ops = block.get("operations") or {}
            # 未知操作盲重放守卫：同输入任一 claimed/unknown 未收敛操作（含跨
            # operation）不得再 claim 重发；owner + expected_revision 已先验证。
            # 返回 changed=False 供 C 只在赢家做 IO，避免 TOCTOU 前置检查绕过。
            unresolved = None
            if isinstance(ops, dict):
                for meta_op in ops.values():
                    if isinstance(meta_op, dict) and \
                            meta_op.get("state") in ("claimed", "unknown"):
                        unresolved = meta_op
                        break
            if unresolved is not None:
                return {
                    "input": d, "changed": False,
                    "revision": unresolved.get("operation_revision", d["revision"]),
                    "reason": "operation-unresolved",
                }
            prev = ops.get(operation)
            st = d["admission_state"]
            from_states = {
                OP_SEND: _SEND_FROM, OP_FLUSH: _FLUSH_FROM, OP_CANCEL: _CANCEL_FROM,
            }[operation]
            if st not in from_states:
                raise LedgerError(
                    "INPUT_ALREADY_ADMITTED" if operation == OP_SEND
                    and st == ADMISSION_ADMITTED
                    else ("ALREADY_DELIVERED" if st == ADMISSION_DELIVERED
                          else "FLUSH_NOT_QUEUED" if operation == OP_FLUSH
                          else "INPUT_TERMINAL"),
                    f"输入 {input_id} 当前状态 {st} 不允许 {operation} 操作")
            rev = d["revision"] + 1
            attempts = int(prev["attempts"]) + 1 if isinstance(prev, dict) else 1
            prev_state = prev.get("state") if isinstance(prev, dict) else None
            ops = dict(ops)
            ops[operation] = {
                "state": "claimed",
                "owner": owner,
                "reason": reason,
                "at": ts,
                "operation_revision": rev,
                "message_id": d["message_id"],
                "attempts": attempts,
                "prior_state": prev_state,
            }
            block = dict(block)
            block["operations"] = ops
            # 状态/计数按 operation 门（send 标记 possible 并计数）。
            new_state = st
            if operation == OP_SEND:
                new_state = ADMISSION_POSSIBLE
            self.conn.execute(
                "UPDATE session_inputs SET revision=?, admission_state=?, "
                "possible_at=?, send_count=?, metadata=?, observed_at=?, "
                "updated_at=? WHERE input_id=?",
                (rev, new_state,
                 ts if operation == OP_SEND else d["possible_at"],
                 (d["send_count"] + 1) if operation == OP_SEND else d["send_count"],
                 json_dumps(dict(d["metadata"], managed_input=block)),
                 ts, ts, input_id),
            )
            self.base._audit(
                self.conn, owner, "managed.claim", "session_input", input_id,
                st, new_state,
                {"operation": operation, "revision": rev,
                 "message_id": d["message_id"], "reason": reason[:200],
                 "attempts": attempts})
            updated = _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone())
            return {"input": updated, "changed": True, "revision": rev}

    # ---- 观察（单调四态；execution/business_ack 仅留档） ---------------
    def record_managed_observation(
        self, input_id: str, *, evidence: dict, now: int,
    ) -> dict:
        if not isinstance(evidence, dict):
            raise LedgerError("INVALID_EVIDENCE", "evidence 必须为对象（dict）")
        ts = _valid_ms(now)
        mid = evidence.get("message_id")
        op = evidence.get("operation")
        op_rev = evidence.get("operation_revision")
        via = evidence.get("via")
        if not _non_empty_str(mid) or not _non_empty_str(op) \
                or not _is_int_not_bool(op_rev):
            raise LedgerError(
                "INVALID_EVIDENCE",
                "evidence 必须含非空 message_id、operation 与整数 operation_revision")
        if op not in ALLOWED_OPERATIONS:
            raise LedgerError("INVALID_EVIDENCE",
                              f"evidence.operation 非法：{op!r}")
        if via is not None and not isinstance(via, str):
            raise LedgerError("INVALID_EVIDENCE", "evidence.via 必须为 str/空")
        has_admission = bool(evidence.get("admission"))
        has_delivery = bool(evidence.get("delivery"))
        has_cancelled = bool(evidence.get("cancelled"))
        has_unknown = bool(evidence.get("unknown"))
        has_execution = evidence.get("execution") is not None
        has_business_ack = evidence.get("business_ack") is not None
        if has_cancelled and evidence.get("trusted") is not True:
            raise LedgerError(
                "UNTRUSTED_CANCELLATION",
                "cancelled 证据必须带 trusted=True（仅 SSE/显式可信取消事件）；"
                "DELETE no-op/异常不得伪造取消")
        if not (has_admission or has_delivery or has_cancelled or has_unknown
                or has_execution or has_business_ack):
            raise LedgerError(
                "INVALID_EVIDENCE",
                "evidence 必须含状态指示（admission/delivery/cancelled/unknown）"
                "或已 delivery 后的 execution/business_ack 归属证据")
        detail = evidence.get("detail")
        if detail is not None and not isinstance(detail, dict):
            raise LedgerError("INVALID_EVIDENCE", "evidence.detail 必须为 dict/空")

        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_managed(d)
            if d["message_id"] != mid:
                raise LedgerError(
                    "INPUT_EVIDENCE_MISMATCH",
                    f"证据 message_id={mid} 与输入 {input_id} 原身份 "
                    f"{d['message_id']} 不一致")
            block = d["metadata"]["managed_input"]
            ops = block.get("operations") or {}
            claim = ops.get(op)
            if not isinstance(claim, dict) or claim.get("operation_revision") != op_rev:
                raise LedgerError(
                    "STALE_OBSERVATION",
                    f"证据归属的 {op} 操作 revision={op_rev} 与当前 claim 不符："
                    "晚到结果不能覆盖新操作",
                    {"stored": (claim or {}).get("operation_revision")})
            cur = d["admission_state"]
            target = cur
            changed = False
            conflict = False
            reason = ""

            if has_delivery:
                if cur == ADMISSION_DELIVERED:
                    reason = "duplicate-delivered"
                elif cur in (ADMISSION_ADMITTED, ADMISSION_POSSIBLE,
                             ADMISSION_PREPARED, ADMISSION_UNKNOWN):
                    target = ADMISSION_DELIVERED
                    changed = True
                elif cur == ADMISSION_CANCELLED:
                    conflict = True
                    reason = "conflict-delivered-after-cancel"
                else:
                    reason = f"terminal:{cur}"
            elif has_cancelled:
                if cur == ADMISSION_CANCELLED:
                    reason = "duplicate-cancelled"
                elif cur in (ADMISSION_PREPARED, ADMISSION_POSSIBLE,
                             ADMISSION_UNKNOWN, ADMISSION_ADMITTED):
                    target = ADMISSION_CANCELLED
                    changed = True
                elif cur == ADMISSION_DELIVERED:
                    conflict = True
                    reason = "conflict-cancel-after-delivered"
                else:
                    reason = f"terminal:{cur}"
            elif has_admission:
                if cur in (ADMISSION_POSSIBLE, ADMISSION_PREPARED,
                           ADMISSION_UNKNOWN):
                    target = ADMISSION_ADMITTED
                    changed = True
                else:
                    reason = "duplicate-admitted" if cur == ADMISSION_ADMITTED \
                        else f"terminal:{cur}"
            elif has_unknown:
                if cur in (ADMISSION_POSSIBLE, ADMISSION_PREPARED,
                           ADMISSION_ADMITTED):
                    target = ADMISSION_UNKNOWN
                    changed = True
                elif cur == ADMISSION_UNKNOWN:
                    reason = "duplicate-unknown"
                else:
                    reason = f"terminal:{cur}"
            else:
                # 仅 execution/business_ack：必须已有 delivered 归属，否则拒绝伪造。
                if cur != ADMISSION_DELIVERED:
                    raise LedgerError(
                        "EXECUTION_EVIDENCE_REQUIRES_DELIVERY",
                        "execution/business_ack 归属证据只允许在已 delivery 后登记；"
                        f"当前 admission_state={cur}",
                        {"admission_state": cur})
                reason = "execution-evidence-only"

            # 事实留档（含冲突事实，供 C 对账）；op 状态推进。
            evidence_block = dict(block.get("evidence") or {})
            fact: dict[str, Any] = {
                "at": ts, "via": via, "operation": op,
                "operation_revision": op_rev, "message_id": mid, "detail": detail,
            }
            if has_admission:
                evidence_block["admitted"] = fact
            if has_delivery:
                evidence_block["delivered"] = fact
            if has_cancelled:
                evidence_block["cancelled"] = fact
            if has_unknown:
                evidence_block["unknown"] = fact
            ack_explicit_conflict = False
            for ref in ("execution", "business_ack"):
                if evidence.get(ref):
                    # 仅观察引用留档（含原引用内容），绝不改写执行证据或业务 ACK。
                    evidence_block[ref] = {
                        "at": ts, "via": via, "operation": op,
                        "operation_revision": op_rev, "message_id": mid,
                        "detail": {"reference": evidence.get(ref)},
                    }
                    if ref == "business_ack":
                        explicit = block.get("business_ack")
                        if isinstance(explicit, dict) and isinstance(
                                explicit.get("value"), bool):
                            incoming = evidence.get(ref)
                            incoming_val = incoming.get("value") if isinstance(
                                incoming, dict) else None
                            if isinstance(incoming_val, bool) and \
                                    incoming_val != explicit["value"]:
                                ack_explicit_conflict = True
            claim = dict(claim)
            claim["state"] = "unknown" if (has_unknown and not has_delivery
                                            and not has_cancelled) else "done"
            ops = dict(ops)
            ops[op] = claim
            block = dict(block)
            block["operations"] = ops
            block["evidence"] = evidence_block
            new_meta = dict(d["metadata"])
            new_meta["managed_input"] = block
            recorded = True

            if changed:
                self.conn.execute(
                    "UPDATE session_inputs SET admission_state=?, "
                    "admitted_at=COALESCE(admitted_at,?), "
                    "delivered_at=COALESCE(delivered_at,?), metadata=?, "
                    "observed_at=?, updated_at=? WHERE input_id=?",
                    (target,
                     ts if target == ADMISSION_ADMITTED else None,
                     ts if target == ADMISSION_DELIVERED else None,
                     json_dumps(new_meta), ts, ts, input_id),
                )
            else:
                # 仅持久证据/操作状态（重复/冲突/终态 no-op 不改 admission_state）。
                self.conn.execute(
                    "UPDATE session_inputs SET metadata=?, observed_at=?, "
                    "updated_at=? WHERE input_id=?",
                    (json_dumps(new_meta), ts, ts, input_id),
                )
            self.base._audit(
                self.conn, "supervisor", "managed.observe", "session_input",
                input_id, cur, target if changed else cur,
                {"operation": op, "operation_revision": op_rev,
                 "message_id": mid, "via": via,
                 "adm": has_admission, "deli": has_delivery,
                 "canc": has_cancelled, "unk": has_unknown,
                 "exec": has_execution, "ack": has_business_ack,
                 "ack_explicit_conflict": ack_explicit_conflict,
                 "conflict": conflict, "reason": reason})
            updated = _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone())
            return {"input": updated, "changed": changed,
                    "recorded": recorded,
                    "admission_state": updated["admission_state"],
                    "conflict": conflict, "reason": reason}

    def record_managed_continuation(
        self, input_id: str, *, report: dict, source_message_id: str,
        via: str = "explicit", now: int,
    ) -> dict:
        """持久结构化 continuation 报告；不改 admission/执行/业务 ACK。

        schema/id 校验由 application/domain 完成；本方法只做受管与形状守卫，
        写 metadata.managed_input.evidence.continuation。
        """
        if not isinstance(report, dict):
            raise LedgerError("INVALID_EVIDENCE", "continuation report 必须是 dict")
        if not _non_empty_str(source_message_id):
            raise LedgerError("INVALID_EVIDENCE",
                              "source_message_id 必须是非空字符串")
        if not isinstance(via, str) or not via.strip():
            raise LedgerError("INVALID_EVIDENCE", "via 必须是非空字符串")
        ts = _valid_ms(now)
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_managed(d)
            block = dict(d["metadata"]["managed_input"])
            evidence_block = dict(block.get("evidence") or {})
            evidence_block["continuation"] = {
                "at": ts, "via": via,
                "source_message_id": source_message_id,
                "report": dict(report),
            }
            block["evidence"] = evidence_block
            new_meta = dict(d["metadata"])
            new_meta["managed_input"] = block
            self.conn.execute(
                "UPDATE session_inputs SET metadata=?, observed_at=?, "
                "updated_at=? WHERE input_id=?",
                (json_dumps(new_meta), ts, ts, input_id),
            )
            self.base._audit(
                self.conn, "runtime", "managed.continuation", "session_input",
                input_id, d["admission_state"], d["admission_state"],
                {"kind": report.get("kind"),
                 "source_message_id": source_message_id, "via": via})
            updated = _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone())
            return {"input": updated,
                    "continuation": evidence_block["continuation"]}

    def note_managed_checked(self, input_id: str, *, now: int,
                             actor: str = "supervisor") -> dict:
        """持久本次对账检查事实（仅 last_checked_at + observed_at/updated_at）。

        不改 revision/admission/正文/claim，供 reconcile_registered_inputs_page；60s 退避使用；query 失败也调用本方法，避免每轮重查同一输入。
        """
        ts = _valid_ms(now)
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_managed(d)
            block = dict(d["metadata"]["managed_input"])
            block["last_checked_at"] = ts
            new_meta = dict(d["metadata"])
            new_meta["managed_input"] = block
            self.conn.execute(
                "UPDATE session_inputs SET metadata=?, observed_at=?, "
                "updated_at=? WHERE input_id=?",
                (json_dumps(new_meta), ts, ts, input_id),
            )
            return {"input": _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone())}

    def acknowledge_managed_input(
        self, input_id: str, *, value: bool, owner: str,
        expected_revision: int, reason: str, now: int,
    ) -> dict:
        """显式业务 ACK 窄事务：owner/状态/expected_revision CAS 一次赢家。

        BEGIN IMMEDIATE 内复核并写 metadata.managed_input.business_ack（bool）+
        revision+1 + audit；任何失败（owner/rev/非法）不回滚已提交外任何写入。
        不关闭后续责任、不改 execution/Task。runtime 的 business_ack 观察不能
        覆盖显式确认；若晚到观察与显式值冲突，保留显式并审计。
        """
        if not isinstance(value, bool):
            raise LedgerError("INVALID_INPUT", "value 必须为 bool")
        if not _non_empty_str(owner):
            raise LedgerError("INVALID_OWNER", "owner 必须为非空字符串")
        if not _non_empty_str(reason):
            raise LedgerError("INVALID_INPUT", "reason 必须为非空字符串")
        if not _is_int_not_bool(expected_revision) or expected_revision < 1:
            raise LedgerError(
                "INPUT_REVISION_CONFLICT",
                f"expected_revision 必须为正整数（收到 {expected_revision!r}）")
        ts = _valid_ms(now)
        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone()
            if row is None:
                raise LedgerError("INPUT_NOT_FOUND", f"输入回执不存在：{input_id}")
            d = _input_from_row(row)
            self._guard_managed(d)
            if d.get("intent") == INTENT_RECORD:
                raise LedgerError("INPUT_TERMINAL",
                                  f"输入 {input_id} 为 record 留档，不支持业务 ACK")
            if d.get("owner") != owner:
                raise LedgerError(
                    "INPUT_OWNER_MISMATCH",
                    f"输入 {input_id} 由 {d.get('owner')} 持有，{owner} 无 ACK 权",
                    {"owner": d.get("owner")})
            if d.get("revision") != expected_revision:
                raise LedgerError(
                    "INPUT_REVISION_CONFLICT",
                    f"输入 {input_id} revision={d.get('revision')} != 期望 "
                    f"{expected_revision}；并发/旧快照拒绝写入",
                    {"revision": d.get("revision"),
                     "expected": expected_revision})
            block = dict(d["metadata"]["managed_input"])
            block["business_ack"] = {
                "value": value, "at": ts, "owner": owner,
                "reason": str(reason)[:500], "source": "explicit",
            }
            new_meta = dict(d["metadata"])
            new_meta["managed_input"] = block
            new_rev = d["revision"] + 1
            self.conn.execute(
                "UPDATE session_inputs SET revision=?, metadata=?, "
                "observed_at=?, updated_at=? WHERE input_id=?",
                (new_rev, json_dumps(new_meta), ts, ts, input_id),
            )
            self.base._audit(
                self.conn, owner, "managed.business_ack", "session_input",
                input_id, d["admission_state"], d["admission_state"],
                {"value": value, "revision": new_rev, "reason": str(reason)[:200]})
            updated = _input_from_row(self.conn.execute(
                "SELECT * FROM session_inputs WHERE input_id=?", (input_id,)
            ).fetchone())
            return {"input": updated, "revision": new_rev, "changed": True}

    # ---- 白名单 -----------------------------------------------------
    @staticmethod
    def _watch_key(session_id: str) -> str:
        return f"{WATCH_KEY_PREFIX}{session_id}"

    @staticmethod
    def _validate_watch(rec: Any, session_id: str) -> dict:
        def bad(msg: str):
            raise LedgerError("WATCH_STATE_CORRUPT",
                              f"受管 watch 记录非法（{session_id}）：{msg}")
        if not isinstance(rec, dict):
            bad("非 dict")
        if rec.get("schema") != WATCH_SCHEMA:
            bad(f"schema={rec.get('schema')!r}")
        if rec.get("session_id") != session_id:
            bad(f"session_id 不匹配 {rec.get('session_id')!r}")
        for key, kind in _WATCH_REQUIRED:
            v = rec.get(key)
            if kind == "int":
                if not _is_int_not_bool(v) or v < 0:
                    bad(f"{key} 非非负整数：{v!r}")
            elif kind == "bool":
                if not isinstance(v, bool):
                    bad(f"{key} 非 bool：{v!r}")
            else:
                if not isinstance(v, str):
                    bad(f"{key} 非 str：{v!r}")
        unreg = rec.get("unregistered_at")
        if unreg is not None and not _is_int_not_bool(unreg):
            bad("unregistered_at 非 int/None")
        last = rec.get("last_observation")
        if last is not None and not isinstance(last, dict):
            bad("last_observation 非 dict/None")
        age_alert = rec.get("age_alert")
        if age_alert is not None:
            if not isinstance(age_alert, dict):
                bad("age_alert 非 dict/None")
            for k in ("session_id", "condition_hash"):
                if not isinstance(age_alert.get(k), str):
                    bad(f"age_alert.{k} 非 str")
            if not _is_int_not_bool(age_alert.get("revision")):
                bad("age_alert.revision 非 int")
        if not isinstance(rec.get("observations"), list):
            bad("observations 非 list")
        for o in rec["observations"]:
            if not isinstance(o, dict):
                bad("observations 内含非 dict 元素")
        return rec

    def _load_watch(self, session_id: str) -> dict | None:
        from .sqlite_db import meta_get
        raw = meta_get(self.conn, self._watch_key(session_id))
        if raw is None:
            return None
        try:
            rec = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise LedgerError(
                "WATCH_STATE_CORRUPT",
                f"受管 watch 记录损坏（JSON）：{session_id}") from exc
        return self._validate_watch(rec, session_id)

    def _store_watch(self, rec: dict) -> None:
        from .sqlite_db import meta_set
        self._validate_watch(rec, rec["session_id"])
        meta_set(self.conn, self._watch_key(rec["session_id"]),
                 json.dumps(rec, ensure_ascii=False))

    def register_managed_watch(self, session_id: str, *, owner: str,
                               project: str, max_pending_age_ms: int,
                               now: int) -> dict:
        if not _non_empty_str(session_id) or not _non_empty_str(owner) \
                or not isinstance(project, str):
            raise LedgerError("INVALID_INPUT",
                              "watch 登记需要非空 session/owner 与 str project")
        if not _is_int_not_bool(max_pending_age_ms) or max_pending_age_ms < 0:
            raise LedgerError("INVALID_INPUT",
                              "max_pending_age_ms 必须为非负整数")
        ts = _valid_ms(now)
        with transaction(self.conn):
            rec = self._load_watch(session_id)
            if rec is None:
                rec = {
                    "schema": WATCH_SCHEMA, "session_id": session_id,
                    "owner": owner, "project": project,
                    "max_pending_age_ms": int(max_pending_age_ms),
                    "enabled": True, "revision": 1,
                    "registered_at": ts, "updated_at": ts,
                    "unregistered_at": None, "last_observation": None,
                    "observations": [],
                }
                self._store_watch(rec)
                return {"watch": rec, "created": True, "changed": True}
            if rec["owner"] != owner:
                raise LedgerError(
                    "WATCH_OWNER_CONFLICT",
                    f"session {session_id} 的 watch 由 {rec['owner']} 登记，"
                    f"{owner} 不能覆盖")
            if (rec.get("enabled") and rec["project"] == project
                    and rec["max_pending_age_ms"] == max_pending_age_ms):
                return {"watch": rec, "created": False, "changed": False}
            # 显式重登记：即使参数完全相同，disabled 也必须恢复观察并递增
            # revision（旧 revision 的晚到观察随即失效，不会复活旧责任）。
            rec = dict(rec)
            rec["project"] = project
            rec["max_pending_age_ms"] = int(max_pending_age_ms)
            rec["enabled"] = True
            rec["unregistered_at"] = None
            rec["revision"] += 1
            rec["updated_at"] = ts
            self._store_watch(rec)
            return {"watch": rec, "created": False, "changed": True}

    def unregister_managed_watch(self, session_id: str, *, owner: str,
                                 expected_revision: int, now: int) -> dict:
        if not _is_int_not_bool(expected_revision):
            raise LedgerError("WATCH_REVISION_CONFLICT",
                              "expected_revision 必须为整数")
        ts = _valid_ms(now)
        with transaction(self.conn):
            rec = self._load_watch(session_id)
            if rec is None:
                raise LedgerError("WATCH_NOT_FOUND",
                                  f"session {session_id} 未登记 watch")
            if rec["owner"] != owner:
                raise LedgerError(
                    "WATCH_OWNER_CONFLICT",
                    f"只有登记 owner {rec['owner']} 可撤销 {session_id} 的 watch")
            if rec["revision"] != expected_revision:
                raise LedgerError(
                    "WATCH_REVISION_CONFLICT",
                    f"watch {session_id} revision={rec['revision']} != "
                    f"期望 {expected_revision}")
            rec = dict(rec)
            rec["enabled"] = False
            rec["unregistered_at"] = ts
            rec["revision"] += 1
            rec["updated_at"] = ts
            self._store_watch(rec)
            return {"watch": rec, "changed": True}

    def get_managed_watch(self, session_id: str) -> dict | None:
        if not _non_empty_str(session_id):
            raise LedgerError("INVALID_INPUT", "session_id 必须为非空字符串")
        return self._load_watch(session_id)

    def list_managed_watches(self, *, limit: int = 100,
                             after_session: str | None = None,
                             enabled_only: bool = False) -> list[dict]:
        """有界白名单列表；兼容旧调用。

        - ``after_session``：session 游标，返回 ``key > prefix+after_session``
          的下一页（升序）；C 持有游标即可轮转，不改 DDL。
        - ``enabled_only=True``：在 SQL ``LIMIT`` 之前用 JSON1 排除 disabled，
          已撤销前缀不会占满窗口；坏 JSON/shape 仍 fail-closed（不 fetchall、
          不放大 limit）。
        - 默认参数行为不变。
        """
        lim = _bounded_limit(limit)
        if after_session is not None and not isinstance(after_session, str):
            raise LedgerError("INVALID_INPUT", "after_session 必须为 str/None")
        sql = "SELECT key, value FROM meta WHERE key LIKE ?"
        params: list[Any] = [WATCH_KEY_PREFIX + "%"]
        if after_session is not None:
            sql += " AND key > ?"
            params.append(WATCH_KEY_PREFIX + after_session)
        if enabled_only:
            # JSON true→1；false→0（排除）；缺 enabled→NULL（保留以触发校验）。
            sql += " AND json_extract(value,'$.enabled') IS NOT 0"
        sql += " ORDER BY key ASC LIMIT ?"
        params.append(lim)
        try:
            rows = self.conn.execute(sql, params).fetchall()
        except sqlite3.DatabaseError as exc:
            raise LedgerError(
                "WATCH_STATE_CORRUPT",
                "受管 watch 列表遇到损坏 JSON，拒绝继续（fail-closed）",
            ) from exc
        out = []
        for r in rows:
            session_id = r["key"][len(WATCH_KEY_PREFIX):]
            try:
                rec = json.loads(r["value"])
            except (TypeError, ValueError) as exc:
                raise LedgerError(
                    "WATCH_STATE_CORRUPT",
                    f"受管 watch 记录损坏（JSON）：{session_id}") from exc
            out.append(self._validate_watch(rec, session_id))
        return out

    def record_managed_watch_observation(
        self, session_id: str, *, expected_revision: int,
        observation: dict, now: int) -> dict:
        if not isinstance(observation, dict):
            raise LedgerError("INVALID_OBSERVATION", "observation 必须为 dict")
        if not _is_int_not_bool(expected_revision):
            raise LedgerError("WATCH_REVISION_CONFLICT",
                              "expected_revision 必须为整数")
        ts = _valid_ms(now)
        with transaction(self.conn):
            rec = self._load_watch(session_id)
            if rec is None:
                return {"recorded": False, "reason": "watch-not-found", "watch": None}
            if not rec.get("enabled"):
                # 撤销后晚到观察：拒绝且绝不重新启用。
                return {"recorded": False, "reason": "watch-disabled",
                        "watch": rec}
            if rec["revision"] != expected_revision:
                raise LedgerError(
                    "WATCH_REVISION_CONFLICT",
                    f"watch {session_id} revision={rec['revision']} != "
                    f"期望 {expected_revision}（观察需使用最新 revision）")
            rec = dict(rec)
            rec["last_observation"] = observation
            obs = list(rec.get("observations") or [])
            obs.append({**observation, "_at": ts})
            if len(obs) > WATCH_MAX_OBSERVATIONS:
                obs = obs[-WATCH_MAX_OBSERVATIONS:]
            rec["observations"] = obs
            rec["updated_at"] = ts
            # 观察为只读事实：不递增 revision，允许多观察者各自写入同一代。
            alert_id = None
            if observation.get("aged_out") is True and \
                    observation.get("trusted") is True:
                alert_id = self._record_watch_age_alert(
                    rec, observation, session_id=session_id, ts=ts)
            self._store_watch(rec)
            return {"recorded": True, "watch": rec, "alert": alert_id}

    def _record_watch_age_alert(self, rec: dict, observation: dict, *,
                                session_id: str, ts: int) -> str | None:
        """aged_out+trusted 观察落持久 owner 告警（同事务原子；ACK 后同条件不重复）。

        去重键：session + watch revision + 稳定 pending 条件哈希（年龄变化不入
        键）。条件标记 ``age_alert`` 与 alerts 行在同一 CAS 事务内写入；已有
        同键 ``state='new'`` 告警时只补标记、不重复造。
        """
        cond_raw = observation.get("pending_condition_key")
        if not _non_empty_str(cond_raw) or len(cond_raw) > 1024:
            raise LedgerError(
                "INVALID_OBSERVATION",
                "aged_out+trusted 观察必须提供 1..1024 字符的 pending_condition_key")
        cond_hash = sha256_text(cond_raw)[:32]
        revision = rec["revision"]
        prev = rec.get("age_alert") or {}
        if (prev.get("session_id") == session_id
                and prev.get("revision") == revision
                and prev.get("condition_hash") == cond_hash):
            return None  # 同一代 + 同一未变条件：ACK 后也不再造。
        dedupe = f"managed-watch-aged:{session_id}:{revision}:{cond_hash}"
        detail = json_dumps({
            "session_id": session_id,
            "owner": rec["owner"],
            "project": rec.get("project"),
            "oldest_age_ms": observation.get("oldest_age_ms"),
            "oldest_message_id": observation.get("oldest_message_id"),
            "pending": observation.get("pending") or observation.get(
                "pending_lower_bound") or observation.get("pending_count"),
            "pending_total": observation.get("pending_total"),
            "pending_lower_bound": observation.get("pending_lower_bound"),
            "truncated": observation.get("truncated"),
            "coverage": observation.get("coverage"),
            "oldest_age_lower_bound_ms": observation.get(
                "oldest_age_lower_bound_ms"),
            "max_pending_age_ms": rec.get("max_pending_age_ms"),
            "next_action": "flush-or-review",
            "watch_revision": revision,
            "condition_key_hash": cond_hash,
        }, ensure_ascii=False)
        alert_id = self._insert_alert_in_tx(
            ts=ts, actor="system", severity="warning",
            kind="managed-watch-aged",
            title=f"白名单 {session_id} 有超期 pending",
            detail=detail, dedupe_key=dedupe)
        rec["age_alert"] = {"session_id": session_id, "revision": revision,
                            "condition_hash": cond_hash, "at": ts,
                            "owner": rec["owner"]}
        return alert_id

    def _insert_alert_in_tx(self, *, ts: int, actor: str, severity: str,
                            kind: str, title: str, detail: str,
                            dedupe_key: str) -> str | None:
        """在当前事务内遵循 alerts 约束写入告警（不嵌套事务）。

        ``alerts_active_dedupe(state='new')`` 已有同键即返回 None（责任已存在），
        否则插入并写 audit。供 watch aged_out 告警与 CAS 标记原子提交。
        """
        existing = self.conn.execute(
            "SELECT id FROM alerts WHERE dedupe_key=? AND state='new' LIMIT 1",
            (dedupe_key,),
        ).fetchone()
        if existing is not None:
            return None
        aid = short_id("AL-", 12)
        self.conn.execute(
            "INSERT INTO alerts(id, severity, kind, title, detail, task_id, "
            "attempt_id, dedupe_key, state, created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (aid, severity, kind, title, detail[:4000], None, None,
             dedupe_key, "new", ts),
        )
        self.base._audit(self.conn, actor, "alert.create", "alert", aid,
                         None, "new", {"severity": severity, "kind": kind})
        return aid

    # ---- 到期告警（只 alert，不 cancel/flush/烧预算） ----------------
    def expire_managed_parks(self, *, now: int, limit: int = 100) -> dict:
        ts = _valid_ms(now)
        lim = _bounded_limit(limit, 100)
        states = (ADMISSION_PREPARED, ADMISSION_POSSIBLE, ADMISSION_UNKNOWN,
                  ADMISSION_ADMITTED)
        ph = ",".join("?" for _ in states)
        # SQL 先筛“有效候选”（未到期前缀/已标前缀不占窗），再切 LIMIT：
        # - expires_at 缺失(NULL) 仍纳入，交由 Python 严格校验 fail-closed；
        # - 同一未变条件的已标行在 SQL 中排除，避免 limit 饿死后排。
        sql = (
            "SELECT * FROM session_inputs WHERE source=? AND intent=? "
            f"AND admission_state IN ({ph}) "
            "AND json_extract(metadata,'$.managed_input.flush.flushed_at') IS NULL "
            "AND (json_extract(metadata,'$.managed_input.expires_at') IS NULL "
            "     OR json_extract(metadata,'$.managed_input.expires_at') < ?) "
            "AND NOT ("
            "  json_extract(metadata,'$.managed_input.expiry_alert.condition_key')"
            "      IS NOT NULL AND "
            "  json_extract(metadata,'$.managed_input.expiry_alert.condition_key')"
            f"      IS ({_PARK_CK_SQL})"
            ") "
            "ORDER BY created_at ASC, input_id ASC LIMIT ?"
        )
        try:
            rows = self.conn.execute(
                sql, (SOURCE_MANAGED_INPUT, INTENT_PARK, *states, ts, lim)
            ).fetchall()
        except sqlite3.DatabaseError as exc:
            # 坏 JSON 等结构错误：fail-closed，不静默跳过。
            raise LedgerError(
                "MANAGED_META_CORRUPT",
                "park 到期扫描遇到损坏 metadata，拒绝继续（fail-closed）",
            ) from exc

        candidates: list[dict] = []
        for row in rows:
            d = _input_from_row(row)
            block = d["metadata"]["managed_input"]
            exp = block.get("expires_at")
            fc = block.get("flush_condition")
            owner = d["owner"]
            if isinstance(block.get("flush"), dict) and \
                    block["flush"].get("flushed_at") is not None:
                continue  # 已 flush 转 execute：不再按 park 到期告警。
            if not _is_int_not_bool(exp) or not _non_empty_str(fc) \
                    or not _non_empty_str(owner) or block.get("expiry_policy") != "alert":
                raise LedgerError(
                    "MANAGED_META_CORRUPT",
                    f"park 输入 {d['input_id']} 缺少 owner/flush_condition/"
                    "expires_at 或 expiry_policy≠alert（不应出现）")
            ck = self._park_condition_key(exp, fc, d["body_hash"])
            prev_alert = block.get("expiry_alert") or {}
            if prev_alert.get("kind") == "park-expired" and \
                    prev_alert.get("condition_key") == ck:
                continue  # 同一未变条件已告警（ACK 后也不重复造）。
            if exp >= ts:
                continue  # 尚未到期。
            candidates.append({"input": d, "condition_key": ck})

        created = 0
        marked = 0
        expired_ids: list[str] = []
        for cand in candidates:
            d = cand["input"]
            ck = cand["condition_key"]
            block = d["metadata"]["managed_input"]
            owner = d["owner"]
            # 去重键含 input_id + 稳定到期条件（哈希），不得按 owner 聚合丢责任。
            suffix = sha256_text(ck)[:16]
            dedupe = (f"managed-park-expired:{owner}:flush:"
                      f"{d['input_id']}:{suffix}")
            alert = self.base.add_alert(
                severity="warning", kind="managed-park-expired",
                title=f"park 输入 {d['session_id']} 已到期未交付",
                detail=json_dumps({
                    "input_id": d["input_id"], "session_id": d["session_id"],
                    "owner": owner, "message_id": d["message_id"],
                    "expires_at": block.get("expires_at"),
                    "flush_condition": block.get("flush_condition"),
                    "next_action": "flush",
                }, ensure_ascii=False),
                dedupe_key=dedupe)
            # 只有本条输入的责任实际可查询后，才允许落标记；否则留给下一轮，
            # 绝不因 add_alert 返回 None 而误标漏告警。
            if alert is None and not self._park_alert_visible(dedupe):
                continue
            if alert is not None:
                created += 1
            with transaction(self.conn):
                row = self.conn.execute(
                    "SELECT * FROM session_inputs WHERE input_id=? AND source=? "
                    "AND intent=?",
                    (d["input_id"], SOURCE_MANAGED_INPUT, INTENT_PARK),
                ).fetchone()
                if row is None:
                    continue
                fresh = _input_from_row(row)
                fb = fresh["metadata"]["managed_input"]
                if fresh["admission_state"] not in states:
                    continue  # 已交付/取消/终态：不告警不改写。
                ck2 = self._park_condition_key(fb["expires_at"],
                                              fb["flush_condition"],
                                              fresh["body_hash"])
                prev_alert = fb.get("expiry_alert") or {}
                if prev_alert.get("condition_key") == ck2:
                    continue
                if ck2 != ck:
                    # 并发改动到期条件：本次告警不覆盖新条件，留待下一轮。
                    continue
                fb = dict(fb)
                fb["expiry_alert"] = {"kind": "park-expired",
                                      "condition_key": ck, "at": ts,
                                      "owner": fresh["owner"]}
                new_meta = dict(fresh["metadata"])
                new_meta["managed_input"] = fb
                self.conn.execute(
                    "UPDATE session_inputs SET metadata=?, updated_at=? "
                    "WHERE input_id=?",
                    (json_dumps(new_meta), ts, fresh["input_id"]))
            marked += 1
            expired_ids.append(d["input_id"])
        return {"checked": len(rows), "alerts": created, "marked": marked,
                "expired": expired_ids}

    def _park_alert_visible(self, dedupe_key: str) -> bool:
        """该去重键对应的告警是否已实际可查询（state='new'）。"""
        row = self.conn.execute(
            "SELECT 1 FROM alerts WHERE dedupe_key=? AND state='new' LIMIT 1",
            (dedupe_key,),
        ).fetchone()
        return row is not None

    @staticmethod
    def _park_condition_key(expires_at: int, flush_condition: str,
                            body_hash: str) -> str:
        return (f"park-expired:v1:{int(expires_at)}:{flush_condition}:"
                f"{str(body_hash)[:16]}")

    # ---- 内部守卫 -----------------------------------------------------
    @staticmethod
    def _guard_managed(d: dict) -> None:
        if d.get("source") != SOURCE_MANAGED_INPUT:
            raise LedgerError(
                "INPUT_NOT_MANAGED",
                f"输入 {d['input_id']} 不是受管输入（source={d.get('source')!r}），"
                "不能用受管操作改写")


__all__ = [
    "SOURCE_MANAGED_INPUT",
    "ADMISSION_RECORDED",
    "OP_SEND", "OP_FLUSH", "OP_CANCEL",
    "ALLOWED_OPERATIONS",
    "MANAGED_META_SCHEMA",
    "WATCH_SCHEMA",
    "WATCH_KEY_PREFIX",
    "MAX_CLOCK_MS",
    "ManagedInputLedger",
]

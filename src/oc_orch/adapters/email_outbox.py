"""独立邮件 outbox：SQLite/meta 实现（operator-email-alerts Requirement 3，D8）。

只复用 ``meta`` 表与 :func:`oc_orch.adapters.sqlite_db.transaction` 短事务，
**不新建业务表、不改公共 schema**；每个 outbox item 持久化为
``mail-outbox:item:{item_id}`` 单条 JSON，schema 版本记录在
``mail-outbox:schema``。本适配器不做任何网络调用。

安全不变量：

- 持久记录在 ``_load``/``_store`` 前后统一严格验证（见 ``_validate_record``）；
  任何坏字段一律抛 :class:`MailOutboxStateError`（fail-closed），不在事务内
  填默认放行，也不静默覆盖损坏记录。校验含：
  - ``max_attempts``/``receipt_limit`` 严格正整数（独立分支，不悬挂在前一 if）；
  - **状态不变量**：``claimed`` 必有 claim_owner/claim_at/possible_submitted_at；
    ``submit-unknown`` 必有 possible_submitted_at；``accepted`` 必须有受理证据
    （submitted_at 或 external_ref）；
  - **内容哈希**：``content_hash`` 必须等于 sha256(subject "\\x00" body)，冻结
    内容不可被改坏而不被发现；
  - 冻结的收件/发件绑定指纹（``recipient_fingerprint``/``sender_fingerprint``）
    非空且不可由观察/登记改写。
- ``item_id`` 即稳定事件身份 ``event_key``：同（project+incident+kind+
  condition_version+收件范围）天然去重，扫描/轮询时间不生成新身份（M06）。
- ``claim`` 在网络副作用**之前**持久化认领者、revision 与 ``possible_submitted_at``，
  并检查**持久 attempt cap**（``attempts >= max_attempts`` 时不再认领，转
  permanent-failed）：崩溃后该行保持“已可能提交”，不因重启被无条件再次
  认领/重发（M09/M12），预算跨重启不重置（M06/M11）。
- 结果写入以 ``expected_revision`` CAS 守卫：晚到回执/并发发送者不能覆盖新
  事件；``claim`` 后同一 item 至多一个发送权（M09）。
- 回执查询预算同样在网络前**原子认领**（``claim_receipt``：owner/revision/
  checks 一步持久化，跨重启不重置，双观察者/崩溃不能无限查询），随后以
  ``expected_receipt_revision`` CAS 提交唯一匹配；已 accepted 的证据不被后写
  覆盖（M10）。
- ``stale_claimed`` 每个候选行在**短事务内重读并改写**：网络晚结果已推进
  revision/状态时不会覆盖（M09/M11）。
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from typing import Any

from ..domain.operator_email import (
    CLAIMABLE_STATUSES,
    EVENT_KINDS,
    REMINDER_KINDS,
    STATUS_ACCEPTED,
    STATUS_AUTH_BLOCKED,
    STATUS_CHANNEL_BLOCKED,
    STATUS_CLAIMED,
    STATUS_EXPIRED_SUPPRESSED,
    STATUS_PENDING,
    STATUS_PENDING_AUTH,
    STATUS_PERMANENT_FAILED,
    STATUS_RATE_LIMITED,
    STATUS_RETRY_WAIT,
    STATUS_SUBMIT_UNKNOWN,
)
from .sqlite_db import meta_get, meta_set, transaction

MAIL_SCHEMA = 1
MAIL_SCHEMA_KEY = "mail-outbox:schema"
MAIL_KEY_PREFIX = "mail-outbox:item:"

MAX_CLOCK_MS = 8 * 10 ** 12
RECEIPT_MATCHES = frozenset({"unique", "none", "ambiguous", "unavailable"})
FAILURE_CLASSES = frozenset({"unknown", "transient", "rate-limited", "auth",
                             "channel", "permanent", "search-unavailable"})

# 源绑定（source binding）字段集：入 outbox / 真实发送 claim 前重验的纯本地
# 台账证据（roadmap R3）。必填字段含归属（source_project/source_task_id），
# 可选会话/模型字段允许 None（用于同 conn 复核项目暂停/回合 hold/provider 冷却）。
SOURCE_KIND_RESPONSIBILITY = "responsibility"
SOURCE_KIND_TASK = "task"
SOURCE_KIND_INCIDENT = "incident"
SOURCE_KINDS = frozenset({SOURCE_KIND_RESPONSIBILITY, SOURCE_KIND_TASK,
                          SOURCE_KIND_INCIDENT})
SOURCE_BINDING_KEYS = ("source_kind", "source_id", "source_condition_key",
                       "source_version", "source_state",
                       "source_incident_type", "source_project",
                       "source_task_id", "source_session", "source_model")
SOURCE_BINDING_REQUIRED_KEYS = ("source_kind", "source_id",
                                "source_condition_key", "source_version",
                                "source_state", "source_incident_type",
                                "source_project", "source_task_id")
SOURCE_BINDING_OPTIONAL_KEYS = ("source_session", "source_model")

_MISSING = object()


def _has_source_binding(rec: dict) -> bool:
    return any(rec.get(k) is not None for k in SOURCE_BINDING_REQUIRED_KEYS)


def _validate_source_binding(value, item_id: str) -> dict | None:
    """严格校验源绑定；必填字段缺失/非字符串/非法 kind → MailOutboxStateError。

    可选字段（source_session/source_model）允许 None，出现时必须为非空字符串
    （拒绝 bool/数字等隐式值）。fail-closed。
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        raise MailOutboxStateError(
            f"mail-outbox 源绑定非法（{item_id!r}）：必须为 dict")
    out: dict = {}
    for key in SOURCE_BINDING_KEYS:
        raw = value.get(key)
        if key in SOURCE_BINDING_REQUIRED_KEYS:
            if not _nk(raw):
                raise MailOutboxStateError(
                    f"mail-outbox 源绑定非法（{item_id!r}）：{key} 必须是非空字符串")
        else:
            if raw is not None and not _nk(raw):
                raise MailOutboxStateError(
                    f"mail-outbox 源绑定非法（{item_id!r}）：{key} 必须是非空字符串或 None")
        out[key] = raw
    if out["source_kind"] not in SOURCE_KINDS:
        raise MailOutboxStateError(
            f"mail-outbox 源绑定非法（{item_id!r}）：source_kind={out['source_kind']!r}")
    return out

_ALL_STATUSES = frozenset({
    STATUS_PENDING_AUTH, STATUS_PENDING, STATUS_CLAIMED, STATUS_SUBMIT_UNKNOWN,
    STATUS_ACCEPTED, STATUS_RETRY_WAIT, STATUS_RATE_LIMITED, STATUS_AUTH_BLOCKED,
    STATUS_CHANNEL_BLOCKED, STATUS_PERMANENT_FAILED, STATUS_EXPIRED_SUPPRESSED,
})


class MailOutboxStateError(RuntimeError):
    """outbox 记录损坏、结构非法或 schema 不兼容。

    调用方必须把该错误视为“状态未知、不得发送”：保留原记录转可见人工处置，
    而不是当作无记录放行。
    """


def _is_int_not_bool(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _valid_ms(value: Any) -> int | None:
    """非负纯数值且在有效时钟边界内；否则返回 None。"""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value if 0 <= value <= MAX_CLOCK_MS else None
    if isinstance(value, float):
        if (not math.isfinite(value) or not value.is_integer()
                or value < 0 or value > MAX_CLOCK_MS):
            return None
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        num = int(value.strip())
        return num if num <= MAX_CLOCK_MS else None
    return None


def _nk(value: Any) -> bool:
    """非空字符串。"""
    return isinstance(value, str) and bool(value.strip())


def _item_key(item_id: str) -> str:
    return f"{MAIL_KEY_PREFIX}{item_id}"


def _content_digest(subject: str, body: str) -> str:
    return hashlib.sha256(f"{subject}\x00{body}".encode("utf-8")).hexdigest()


def _validate_record(rec: dict) -> None:
    """持久记录结构性验证；任何坏字段抛 MailOutboxStateError（fail-closed）。"""

    def bad(msg: str):
        raise MailOutboxStateError(f"mail-outbox 记录非法（{rec.get('item_id')!r}）：{msg}")

    if rec.get("schema") != MAIL_SCHEMA:
        bad(f"schema={rec.get('schema')!r}")
    item_id = rec.get("item_id")
    if not _nk(item_id):
        bad("item_id 缺失")
    if rec.get("event_key") != item_id:
        bad("event_key != item_id（事件身份不一致）")
    for key in ("base_key", "project", "incident_id", "incident_type", "kind",
                "recipient_ref", "sender_ref", "recipient_fingerprint",
                "sender_fingerprint", "subject", "body", "content_hash"):
        if not _nk(rec.get(key)):
            bad(f"缺少非空字段 {key}")
    if rec.get("kind") not in EVENT_KINDS:
        bad(f"kind={rec.get('kind')!r}")
    # 源绑定（可选）：必填字段同现同缺（非空字符串），可选字段 None 或非空字符串。
    src_present = [k for k in SOURCE_BINDING_REQUIRED_KEYS
                   if rec.get(k) is not None]
    if src_present and len(src_present) != len(SOURCE_BINDING_REQUIRED_KEYS):
        bad("source 必填绑定字段必须同时存在或同时缺失")
    for key in SOURCE_BINDING_REQUIRED_KEYS:
        value = rec.get(key)
        if value is not None and not _nk(value):
            bad(f"{key} 必须是非空字符串")
    for key in SOURCE_BINDING_OPTIONAL_KEYS:
        value = rec.get(key)
        if value is not None and not _nk(value):
            bad(f"{key} 必须是非空字符串或 None")
    if rec.get("source_kind") is not None \
            and rec["source_kind"] not in SOURCE_KINDS:
        bad(f"source_kind={rec.get('source_kind')!r}")
    if not _is_int_not_bool(rec.get("condition_version")) \
            or rec["condition_version"] < 1:
        bad("condition_version 非法")
    if not _is_int_not_bool(rec.get("escalation_level")) \
            or rec["escalation_level"] < 0:
        bad("escalation_level 非法")
    if rec.get("status") not in _ALL_STATUSES:
        bad(f"status={rec.get('status')!r}")
    if not _is_int_not_bool(rec.get("revision")) or rec["revision"] < 1:
        bad("revision 非法")
    for key in ("claim_at", "possible_submitted_at", "submitted_at",
                "deadline_at", "receipt_claim_at"):
        value = rec.get(key)
        if value is not None and _valid_ms(value) is None:
            bad(f"{key} 超出有效时钟边界：{value!r}")
    if rec.get("claim_owner") is not None and not isinstance(
            rec.get("claim_owner"), str):
        bad("claim_owner 非 str/None")
    if rec.get("receipt_claim_owner") is not None and not isinstance(
            rec.get("receipt_claim_owner"), str):
        bad("receipt_claim_owner 非 str/None")
    if rec.get("external_ref") is not None and not isinstance(
            rec.get("external_ref"), str):
        bad("external_ref 非 str/None")
    if rec.get("receipt_match") is not None \
            and rec["receipt_match"] not in RECEIPT_MATCHES:
        bad(f"receipt_match={rec.get('receipt_match')!r}")
    for key in ("receipt_checks", "attempts", "next_attempt_at"):
        value = rec.get(key)
        if not _is_int_not_bool(value) or value < 0:
            bad(f"{key} 非法：{value!r}")
    if rec.get("retry_after_ms") is not None \
            and (not _is_int_not_bool(rec["retry_after_ms"])
                 or rec["retry_after_ms"] < 0):
        bad("retry_after_ms 非法")
    # 严格正整数预算（独立分支，不悬挂在前一 if 内——曾误缩进导致漏检）。
    if not _is_int_not_bool(rec.get("max_attempts")) or rec["max_attempts"] < 1:
        bad("max_attempts 非法")
    if not _is_int_not_bool(rec.get("receipt_limit")) or rec["receipt_limit"] < 1:
        bad("receipt_limit 非法")
    if not _is_int_not_bool(rec.get("receipt_revision")) \
            or rec["receipt_revision"] < 1:
        bad("receipt_revision 非法")
    if rec.get("failure_class") is not None \
            and rec["failure_class"] not in FAILURE_CLASSES:
        bad(f"failure_class={rec.get('failure_class')!r}")
    if rec.get("last_error") is not None and not isinstance(rec.get("last_error"),
                                                            str):
        bad("last_error 非 str/None")
    for key in ("first_seen_at", "last_observed_at", "created_at", "updated_at"):
        value = rec.get(key)
        if _valid_ms(value) is None:
            bad(f"{key} 非法：{value!r}")
    for key in ("summary", "metadata"):
        if not isinstance(rec.get(key), dict):
            bad(f"{key} 非 dict")
    # 内容哈希必须与冻结的 subject/body 匹配（不可变内容摘要）。
    if _content_digest(rec["subject"], rec["body"]) != rec["content_hash"]:
        bad("content_hash 与冻结内容不匹配")
    # 时间一致性：更新/观察不早于创建；认领不早于创建。
    if rec["updated_at"] < rec["created_at"]:
        bad("updated_at < created_at")
    if rec["last_observed_at"] < rec["created_at"]:
        bad("last_observed_at < created_at")
    # ---- 状态不变量 -----------------------------------------------------
    status = rec["status"]
    if status == STATUS_CLAIMED:
        if not _nk(rec.get("claim_owner")) or _valid_ms(rec.get("claim_at")) is None \
                or _valid_ms(rec.get("possible_submitted_at")) is None:
            bad("claimed 状态缺少 claim_owner/claim_at/possible_submitted_at")
    if status == STATUS_SUBMIT_UNKNOWN:
        if _valid_ms(rec.get("possible_submitted_at")) is None:
            bad("submit-unknown 状态缺少 possible_submitted_at（可能已提交证据）")
    if status == STATUS_ACCEPTED:
        if rec.get("submitted_at") is None and not _nk(rec.get("external_ref")):
            bad("accepted 状态缺少受理证据（submitted_at 或 external_ref）")
    if status != STATUS_CLAIMED:
        if rec.get("claim_owner") is not None or _valid_ms(rec.get("claim_at")) \
                is not None:
            bad(f"{status} 状态不应保留 claim_owner/claim_at")


def _load(conn: sqlite3.Connection, item_id: str) -> dict | None:
    raw = meta_get(conn, _item_key(item_id))
    if raw is None:
        return None
    try:
        rec = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise MailOutboxStateError(
            f"mail-outbox 记录损坏（JSON）：{item_id}") from exc
    if not isinstance(rec, dict):
        raise MailOutboxStateError(
            f"mail-outbox 记录损坏（非 dict）：{item_id}")
    _validate_record(rec)
    return rec


def _store(conn: sqlite3.Connection, rec: dict) -> None:
    _validate_record(rec)
    meta_set(conn, _item_key(rec["item_id"]),
             json.dumps(rec, ensure_ascii=False))


def _snapshot(rec: dict) -> dict:
    return dict(rec)


class EmailOutbox:
    """meta 命名空间持久 outbox（最小 API 见 application/operator_email）。

    ``source_validator``（可选注入）：纯本地源绑定重验窄端口
    ``callable(conn, item) -> {"ok": bool, "reason": str}``（只读同一 conn，
    禁止任意网络回调）。配置后：

    - ``upsert_item`` 在**创建/升级行事务内**重验（入 outbox 前重验原 source
      归属/条件版本/人工控制），失败 → 不建行（fail-closed）；
    - ``claim`` 在**认领事务内**重验（真实发送网络副作用前），失败 → 转
      ``expired-suppressed`` 且拒绝认领（不发送、不忙循环）；
    - ``require_source_binding=True`` 时，任何缺少源绑定字段的行一律拒绝
      认领（真实启用路径必须携带可重验的台账证据，不能用调用方自报 bool）。
    """

    def __init__(self, conn: sqlite3.Connection, clock: Any, *,
                 source_validator: Any = None,
                 require_source_binding: bool = False):
        self._conn = conn
        self._clock = clock
        self._source_validator = source_validator
        self._require_source_binding = bool(require_source_binding)

    def _recheck_ok(self, rec: dict) -> tuple[bool, str, str]:
        """在**当前事务内**调用源绑定重验器（纯本地）。返回 (ok, reason, state)。

        - ``require_source_binding=True`` 且未注入 validator：一律拒绝
          （``source-validator-unavailable``）——字段存在性不是真实源证明，
          调用方自报“完整绑定”不被当作台账证据；
        - validator 返回必须严格 ``ok is True``；不严格 ok 或异常一律保守拒绝
          （不用 bool coercion），state 取五态之一或按 unknown fail-closed。
        """
        state_unknown = "unknown"
        if self._require_source_binding and self._source_validator is None:
            return False, "source-validator-unavailable", state_unknown
        if not _has_source_binding(rec):
            if self._require_source_binding:
                return False, "source-binding-required", state_unknown
            return True, "no-source-binding", "active"
        if self._source_validator is None:
            return True, "no-validator", "active"
        try:
            verdict = self._source_validator(self._conn, rec)
        except Exception:
            verdict = None
        state = verdict.get("state") if isinstance(verdict, dict) else state_unknown
        if (not isinstance(verdict, dict) or verdict.get("ok") is not True
                or state not in (None, "active")
                or self._require_source_binding and state != "active"):
            reason = (verdict or {}).get("reason") if isinstance(verdict, dict) \
                else "invalid-validator-result"
            state = verdict.get("state") if isinstance(verdict, dict) \
                else state_unknown
            if state not in ("active", "resolved", "stale", "paused", "unknown"):
                state = state_unknown
            return False, str(reason or "source-recheck-failed"), state
        return True, "verified", "active"

    # ---- 内部 ----------------------------------------------------------
    def _now_ms(self) -> int:
        return int(self._clock.now_ms())

    def _read_schema(self) -> None:
        raw = meta_get(self._conn, MAIL_SCHEMA_KEY)
        if raw is None:
            return
        try:
            current = int(raw)
        except (TypeError, ValueError) as exc:
            raise MailOutboxStateError(
                f"mail-outbox schema 记录损坏：{raw!r}") from exc
        if current != MAIL_SCHEMA:
            raise MailOutboxStateError(
                f"mail-outbox schema 不兼容：{current} != {MAIL_SCHEMA}")

    def _ensure_schema(self) -> None:
        self._read_schema()
        if meta_get(self._conn, MAIL_SCHEMA_KEY) is None:
            meta_set(self._conn, MAIL_SCHEMA_KEY, str(MAIL_SCHEMA))

    # ---- 登记/去重 -----------------------------------------------------
    def upsert_item(self, *, item_id: str, base_key: str, event_key: str,
                    project: str, incident_id: str, incident_type: str,
                    kind: str, condition_version: int, escalation_level: int,
                    recipient_ref: str, sender_ref: str,
                    recipient_fingerprint: str, sender_fingerprint: str,
                    subject: str, body: str, content_hash: str, status: str,
                    first_seen_at: int, observed_at: int, summary: dict,
                    max_attempts: int, receipt_limit: int,
                    deadline_at: int | None, now: int,
                    metadata: dict | None = None,
                    source_binding: dict | None = None) -> dict:
        """按稳定事件身份登记；已存在 → 只刷新观察（不重复建事件身份）。

        返回 ``{"created": bool, "changed": bool, "item": snapshot}``。
        幂等：同 event_key 重复调用不建新行；``pending-auth`` 行在后续授权
        到位时自动升级为 ``pending``（status 参数为 pending 时），并用本次
        注入的绑定指纹刷新（授权后绑定才冻结为可发送配置）。

        ``source_binding``（可选）：入 outbox 前在同一事务内重验的源绑定
        （见 ``_recheck_ok``）；建立新行/授权升级时若重验失败 → 不建行
        （fail-closed），返回 ``reason=source-recheck-failed``。旧行不存在源
        字段且传入新绑定 → 身份冲突（fail-closed，避免给旧事件补绑定）。
        """
        if item_id != event_key:
            return {"created": False, "changed": False,
                    "reason": "item-event-mismatch", "item": None}
        binding = _validate_source_binding(source_binding, item_id)
        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, item_id)
            if rec is None:
                rec = {
                    "schema": MAIL_SCHEMA,
                    "item_id": item_id,
                    "base_key": base_key,
                    "event_key": event_key,
                    "project": project,
                    "incident_id": incident_id,
                    "incident_type": incident_type,
                    "kind": kind,
                    "condition_version": condition_version,
                    "escalation_level": escalation_level,
                    "recipient_ref": recipient_ref,
                    "sender_ref": sender_ref,
                    "recipient_fingerprint": recipient_fingerprint,
                    "sender_fingerprint": sender_fingerprint,
                    "source_kind": binding["source_kind"] if binding else None,
                    "source_id": binding["source_id"] if binding else None,
                    "source_condition_key": binding["source_condition_key"]
                    if binding else None,
                    "source_version": binding["source_version"] if binding else None,
                    "source_state": binding["source_state"] if binding else None,
                    "source_incident_type": binding["source_incident_type"]
                    if binding else None,
                    "source_project": binding["source_project"] if binding else None,
                    "source_task_id": binding["source_task_id"] if binding else None,
                    "source_session": binding["source_session"] if binding else None,
                    "source_model": binding["source_model"] if binding else None,
                    "subject": subject,
                    "body": body,
                    "content_hash": content_hash,
                    "status": status,
                    "revision": 1,
                    "claim_owner": None,
                    "claim_at": None,
                    "possible_submitted_at": None,
                    "submitted_at": None,
                    "external_ref": None,
                    "receipt_match": None,
                    "receipt_checks": 0,
                    "receipt_revision": 1,
                    "receipt_limit": receipt_limit,
                    "receipt_claim_owner": None,
                    "receipt_claim_at": None,
                    "attempts": 0,
                    "max_attempts": max_attempts,
                    "next_attempt_at": now,
                    "deadline_at": deadline_at,
                    "retry_after_ms": None,
                    "failure_class": None,
                    "last_error": None,
                    "first_seen_at": first_seen_at,
                    "last_observed_at": observed_at,
                    "summary": dict(summary) if isinstance(summary, dict) else {},
                    "created_at": now,
                    "updated_at": now,
                    "metadata": dict(metadata) if isinstance(metadata, dict) else {},
                }
                # 入 outbox 前重验源（同一事务内：源重验 + 可能提交原子）。
                ok, reason, _state = self._recheck_ok(rec)
                if not ok:
                    return {"created": False, "changed": False,
                            "reason": "source-recheck-failed",
                            "recheck_reason": reason, "item": None}
                _store(self._conn, rec)
                return {"created": True, "changed": True, "reason": "created",
                        "item": _snapshot(rec)}
            if rec["kind"] != kind or rec["revision"] < 1:
                # 同 event_key 但身份不一致 → 损坏记录级冲突，fail-closed。
                raise MailOutboxStateError(
                    f"mail-outbox 事件身份冲突：{item_id}")
            if binding is not None and rec.get("source_kind") is None:
                # 旧行从未携带源绑定：不得用新绑定给旧事件补身份（fail-closed）。
                raise MailOutboxStateError(
                    f"mail-outbox 源绑定冲突（旧行无绑定）：{item_id}")
            if binding is not None and (
                    rec.get("source_kind") != binding["source_kind"]
                    or rec.get("source_id") != binding["source_id"]
                    or rec.get("source_project") != binding["source_project"]):
                raise MailOutboxStateError(
                    f"mail-outbox 源绑定冲突（事件身份不一致）：{item_id}")
            changed = False
            if rec["status"] == STATUS_PENDING_AUTH and status == STATUS_PENDING:
                # 授权后来到：等待行可进入发送队列（不发新事件），
                # 且以本次配置绑定刷新 frozen ref/指纹（此前从未发送过）。
                rec["status"] = STATUS_PENDING
                rec["recipient_ref"] = recipient_ref
                rec["sender_ref"] = sender_ref
                rec["recipient_fingerprint"] = recipient_fingerprint
                rec["sender_fingerprint"] = sender_fingerprint
                rec["revision"] += 1
                changed = True
                # 授权升级前重验源（同一事务内；失败保持 pending-auth 可见）。
                ok, reason, _state = self._recheck_ok(rec)
                if not ok:
                    rec["status"] = STATUS_PENDING_AUTH
                    rec["revision"] -= 1
                    return {"created": False, "changed": False,
                            "reason": "source-recheck-failed",
                            "recheck_reason": reason,
                            "item": _snapshot(rec)}
            if observed_at > rec["last_observed_at"]:
                rec["last_observed_at"] = observed_at
                changed = True
            if isinstance(summary, dict) and summary:
                rec["summary"] = dict(summary)
                changed = True
            if changed:
                rec["updated_at"] = max(rec["updated_at"], now)
                _store(self._conn, rec)
            return {"created": False, "changed": changed,
                    "reason": "duplicate-observation", "item": _snapshot(rec)}

    # ---- 读取 ----------------------------------------------------------
    def get_item(self, item_id: str) -> dict | None:
        self._read_schema()
        rec = _load(self._conn, item_id)
        return _snapshot(rec) if rec else None

    def items_for_base(self, base_key: str) -> list[dict]:
        """同一 incident 的全部行（用于事件选择/恢复抑制）。"""
        self._read_schema()
        rows = self._conn.execute(
            "SELECT key FROM meta WHERE key LIKE ?", (MAIL_KEY_PREFIX + "%",)
        ).fetchall()
        out = []
        for row in rows:
            key = row["key"]
            item_id = key[len(MAIL_KEY_PREFIX):]
            if not item_id:
                continue
            rec = _load(self._conn, item_id)
            if rec is not None and rec.get("base_key") == base_key:
                out.append(_snapshot(rec))
        return sorted(out, key=lambda r: (r["created_at"], r["item_id"]))

    def list_due(self, *, now: int, limit: int = 10) -> list[dict]:
        """可发送的候选项：待发/重试等待/限频等待且已到期、未过期、未超预算。"""
        self._read_schema()
        rows = self._conn.execute(
            "SELECT key FROM meta WHERE key LIKE ?", (MAIL_KEY_PREFIX + "%",)
        ).fetchall()
        due = []
        for row in rows:
            key = row["key"]
            item_id = key[len(MAIL_KEY_PREFIX):]
            if not item_id:
                continue
            rec = _load(self._conn, item_id)
            if rec is None or rec["status"] not in CLAIMABLE_STATUSES:
                continue
            if rec["next_attempt_at"] > now:
                continue
            if rec.get("attempts", 0) >= rec.get("max_attempts", 1):
                continue  # 持久 attempt cap 已耗尽 → 交 claim/恢复流程转终态
            deadline = rec.get("deadline_at")
            if deadline is not None and now > deadline \
                    and rec.get("possible_submitted_at") is None:
                continue  # 已过发送期限且未可能提交 → 交 expire 流程抑制
            due.append(_snapshot(rec))
        due.sort(key=lambda r: (r["created_at"], r["item_id"]))
        return due[:limit]

    def list_all(self) -> list[dict]:
        self._read_schema()
        rows = self._conn.execute(
            "SELECT key FROM meta WHERE key LIKE ?", (MAIL_KEY_PREFIX + "%",)
        ).fetchall()
        out = []
        for row in rows:
            key = row["key"]
            item_id = key[len(MAIL_KEY_PREFIX):]
            if not item_id:
                continue
            rec = _load(self._conn, item_id)
            if rec is not None:
                out.append(_snapshot(rec))
        return sorted(out, key=lambda r: (r["created_at"], r["item_id"]))

    # ---- 认领（网络前；持久 possible 标记 + attempt cap） ---------------
    def claim(self, item_id: str, *, owner: str, now: int) -> dict:
        """原子认领一个到期行。

        规则（M09/M11）：
        - 状态必须在 ``pending/retry-wait/rate-limited`` 且已到期；
        - 持久 attempt cap：``attempts >= max_attempts`` → 不再认领，转
          permanent-failed（跨重启不清零）；
        - 未过期（deadline 未过，或已可能提交）；
        - 认领即持久化 ``claim_owner/claim_at/possible_submitted_at`` 并
          revision += 1 —— 网络副作用之前完成，崩溃后保持“已可能提交”，
          不因重启再次无条件提交；
        - ``claimed/submit-unknown/accepted`` 永不获再次认领。
        """
        if not (_nk(owner) and _nk(item_id)):
            return {"allowed": False, "reason": "invalid-claim-ids",
                    "item": None}
        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, item_id)
            if rec is None:
                return {"allowed": False, "reason": "missing", "item": None}
            if rec["status"] not in CLAIMABLE_STATUSES:
                return {"allowed": False, "reason": "not-claimable",
                        "item": _snapshot(rec)}
            if rec.get("attempts", 0) >= rec.get("max_attempts", 1):
                rec["status"] = STATUS_PERMANENT_FAILED
                rec["last_error"] = "attempt-budget-exhausted"
                rec["failure_class"] = "permanent"
                rec["revision"] += 1
                rec["updated_at"] = now
                _store(self._conn, rec)
                return {"allowed": False, "reason": "attempt-budget-exhausted",
                        "item": _snapshot(rec)}
            if rec["next_attempt_at"] > now:
                return {"allowed": False, "reason": "not-due",
                        "item": _snapshot(rec)}
            deadline = rec.get("deadline_at")
            if deadline is not None and now > deadline \
                    and rec.get("possible_submitted_at") is None:
                rec["status"] = STATUS_EXPIRED_SUPPRESSED
                rec["last_error"] = "deadline-expired-before-submit"
                rec["revision"] += 1
                rec["updated_at"] = now
                _store(self._conn, rec)
                return {"allowed": False, "reason": "expired",
                        "item": _snapshot(rec)}
            # ---- 网络前重验源（同一事务内：源重验 + 可能提交 claim 原子）---
            ok, recheck_reason, vstate = self._recheck_ok(rec)
            if not ok:
                if vstate in ("paused", "unknown"):
                    # 正常暂停/回合 hold/冷却，或读不到/坏数据：不发送、不写
                    # 状态（等待后续/保持 fail-closed），绝不当作恢复。
                    return {"allowed": False, "reason": f"source-{vstate}",
                            "recheck_reason": recheck_reason,
                            "item": _snapshot(rec)}
                # resolved/stale：该提醒已失效（源解除或条件改变）→ 转可见终态，
                # 不发送且不忙循环。
                rec["status"] = STATUS_EXPIRED_SUPPRESSED
                rec["last_error"] = f"source-stale:{recheck_reason}"[:200]
                rec["revision"] += 1
                rec["updated_at"] = now
                _store(self._conn, rec)
                return {"allowed": False, "reason": "source-stale",
                        "recheck_reason": recheck_reason,
                        "item": _snapshot(rec)}
            rec["status"] = STATUS_CLAIMED
            rec["claim_owner"] = owner
            rec["claim_at"] = now
            rec["possible_submitted_at"] = now
            rec["last_error"] = None
            rec["revision"] += 1
            rec["updated_at"] = now
            _store(self._conn, rec)
            return {"allowed": True, "reason": "claimed",
                    "revision": rec["revision"], "item": _snapshot(rec)}

    # ---- 结果写入（CAS 守卫） ------------------------------------------
    def mark_result(self, item_id: str, *, expected_revision: int,
                    change: dict, now: int) -> dict:
        """以 claim 后的 revision 提交发送结果；并发/晚到写入被拒绝。

        ``change`` 只接受白名单字段（不含回执相关字段，回执走独立 CAS）；
        写入前整体过 ``_validate_record``（fail-closed，不落 invalid 状态）。
        """
        allowed_change_keys = frozenset({
            "status", "attempts", "next_attempt_at", "possible_submitted_at",
            "submitted_at", "retry_after_ms", "failure_class", "last_error",
            "deadline_at",
        })
        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, item_id)
            if rec is None:
                return {"applied": False, "reason": "missing", "item": None}
            if not _is_int_not_bool(expected_revision) \
                    or rec["revision"] != expected_revision:
                return {"applied": False, "reason": "revision-changed",
                        "item": _snapshot(rec)}
            if not isinstance(change, dict):
                return {"applied": False, "reason": "invalid-change",
                        "item": _snapshot(rec)}
            for key, value in change.items():
                if key not in allowed_change_keys:
                    continue
                if key == "attempts" and not _is_int_not_bool(value):
                    continue
                if key in ("next_attempt_at", "retry_after_ms") \
                        and _valid_ms(value) is None and value is not None:
                    continue
                rec[key] = value
            if rec["status"] != STATUS_CLAIMED:
                # 离开在途状态：回收 claim 所有权（不变量：非 claimed 无 owner）。
                rec["claim_owner"] = None
                rec["claim_at"] = None
            rec["revision"] += 1  # 结果以版本守卫提交：晚到/并发结果被拒绝
            rec["updated_at"] = now
            _store(self._conn, rec)
            return {"applied": True, "reason": "result-recorded",
                    "item": _snapshot(rec)}

    # ---- 观察摘要刷新（不改状态/revision） -----------------------------
    def update_observation(self, item_id: str, *, observed_at: int,
                           summary: dict, now: int) -> dict:
        """重复观察只刷新摘要/first_seen/last_observed；不改变发送状态。"""
        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, item_id)
            if rec is None:
                return {"applied": False, "reason": "missing", "item": None}
            changed = False
            if isinstance(summary, dict) and summary:
                rec["summary"] = dict(summary)
                changed = True
            if observed_at > rec["last_observed_at"]:
                rec["last_observed_at"] = observed_at
                changed = True
            if changed:
                rec["updated_at"] = max(rec["updated_at"], now)
                _store(self._conn, rec)
            return {"applied": True, "changed": changed,
                    "item": _snapshot(rec)}

    # ---- 授权阻塞 ------------------------------------------------------
    def auth_block(self, item_id: str, *, reason: str, now: int) -> dict:
        """授权失效/越界 → 可见 auth-blocked；不自动重试、不写新事件。"""
        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, item_id)
            if rec is None:
                return {"applied": False, "reason": "missing", "item": None}
            blockable = frozenset({
                STATUS_PENDING_AUTH, STATUS_PENDING, STATUS_RETRY_WAIT,
                STATUS_RATE_LIMITED})
            if rec["status"] not in blockable:
                return {"applied": False, "reason": "not-blockable",
                        "item": _snapshot(rec)}
            if rec["status"] != STATUS_AUTH_BLOCKED:
                rec["status"] = STATUS_AUTH_BLOCKED
                rec["revision"] += 1
            rec["last_error"] = str(reason)[:200]
            rec["updated_at"] = now
            _store(self._conn, rec)
            return {"applied": True, "reason": "authority-blocked",
                    "item": _snapshot(rec)}

    # ---- 过期抑制（M12：恢复发生在未可能提交之前） ---------------------
    def expire_unsubmitted(self, *, base_key: str, now: int,
                           reason: str = "recovered-before-possible-submit"
                           ) -> int:
        """抑制同一 incident 下“尚未可能提交”的提醒（不含 recovery 行）。

        含 ``pending-auth``（从未获授权）与可认领家族；已可能提交的行
        （possible_submitted_at 非空）不可撤回，保持待回执。返回被抑制行数。
        """
        count = 0
        with transaction(self._conn):
            self._ensure_schema()
            rows = self._conn.execute(
                "SELECT key FROM meta WHERE key LIKE ?", (MAIL_KEY_PREFIX + "%",)
            ).fetchall()
            for row in rows:
                key = row["key"]
                item_id = key[len(MAIL_KEY_PREFIX):]
                if not item_id:
                    continue
                rec = _load(self._conn, item_id)
                if rec is None or rec.get("base_key") != base_key:
                    continue
                if rec.get("kind") in REMINDER_KINDS \
                        and rec["status"] in (CLAIMABLE_STATUSES
                                              | frozenset({STATUS_PENDING_AUTH})) \
                        and rec.get("possible_submitted_at") is None:
                    rec["status"] = STATUS_EXPIRED_SUPPRESSED
                    rec["last_error"] = reason
                    rec["revision"] += 1
                    rec["updated_at"] = now
                    _store(self._conn, rec)
                    count += 1
        return count

    # ---- 陈旧认领（崩溃/sidecar 死亡后不盲重发） -----------------------
    def stale_claimed(self, *, now: int, stale_after_ms: int) -> list[dict]:
        """超过认领时限仍 claimed 的行：转 submit-unknown 等待回执补证。

        每个候选行在**短事务内重读并改写**：若网络晚结果已提交（status 已变、
        revision 已推进），事务内重读会看到非 claimed → 跳过，不覆盖晚结果。
        """
        self._read_schema()
        rows = self._conn.execute(
            "SELECT key FROM meta WHERE key LIKE ?", (MAIL_KEY_PREFIX + "%",)
        ).fetchall()
        out = []
        for row in rows:
            key = row["key"]
            item_id = key[len(MAIL_KEY_PREFIX):]
            if not item_id:
                continue
            with transaction(self._conn):
                self._ensure_schema()
                rec = _load(self._conn, item_id)  # 事务内重读
                if rec is None or rec["status"] != STATUS_CLAIMED:
                    continue
                claim_at = rec.get("claim_at") or 0
                if now - claim_at < stale_after_ms:
                    continue
                rec["status"] = STATUS_SUBMIT_UNKNOWN
                rec["claim_owner"] = None
                rec["claim_at"] = None
                rec["last_error"] = "stale-claim-reconciled-waiting-receipt"
                rec["revision"] += 1
                rec["updated_at"] = now
                _store(self._conn, rec)
                out.append(_snapshot(rec))
        return out

    # ---- 回执查询 claim（M10：网络前原子消耗预算，双观察者/崩溃有限） ----
    def claim_receipt(self, item_id: str, *, owner: str, now: int) -> dict:
        """网络副作用之前原子认领一次回执查询：预算/owner/revision 一步持久。

        - 只在 ``submit-unknown``/``claimed``（可能已提交）行上可查询；
        - 预算 ``receipt_checks >= receipt_limit`` → 拒绝（跨重启不重置，
          换策略配置也不能给旧行加预算——limit 冻结在行内）；
        - 成功后 ``receipt_revision += 1``，后续 ``record_receipt`` 必须携带
          该 revision，任何后写都不能覆盖已 accepted 的证据。
        """
        if not (_nk(owner) and _nk(item_id)):
            return {"allowed": False, "reason": "invalid-claim-ids",
                    "receipt_revision": None, "item": None}
        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, item_id)
            if rec is None:
                return {"allowed": False, "reason": "missing",
                        "receipt_revision": None, "item": None}
            if rec["status"] not in (STATUS_SUBMIT_UNKNOWN, STATUS_CLAIMED):
                return {"allowed": False, "reason": "not-applicable",
                        "receipt_revision": None, "item": _snapshot(rec)}
            count = (rec.get("receipt_checks") or 0) + 1
            limit = rec.get("receipt_limit", 1)
            if count > limit:
                return {"allowed": False, "reason": "receipt-check-exhausted",
                        "receipt_revision": None, "item": _snapshot(rec)}
            rec["receipt_checks"] = count
            rec["receipt_claim_owner"] = owner
            rec["receipt_claim_at"] = now
            rec["receipt_revision"] = (rec.get("receipt_revision") or 0) + 1
            rec["updated_at"] = now
            _store(self._conn, rec)
            return {"allowed": True, "reason": "receipt-claimed",
                    "receipt_revision": rec["receipt_revision"],
                    "item": _snapshot(rec)}

    # ---- 回执结果（M10：唯一匹配 + CAS，不覆盖已 accepted 证据） --------
    def record_receipt(self, item_id: str, *, expected_receipt_revision: int,
                       receipt_match: str, external_ref: str | None,
                       now: int) -> dict:
        """以 ``claim_receipt`` 的 receipt_revision 提交查询结果。

        - ``unique``：补记受理证据（external_ref）并转 ``accepted``；若早已
          accepted 且有证据 → 幂等且**不覆盖**既有证据；
        - ``none``/``ambiguous``：记录查询事实，状态保持（submit-unknown），
          不重发（无匹配不等于未发送）；
        - ``unavailable``：查询失败本身，不记新结论；
        - CAS：receipt_revision 不匹配（有并发查询/结果已写）→ 拒绝，晚到
          回执不覆盖新证据/新事件。
        """
        if receipt_match not in RECEIPT_MATCHES:
            return {"applied": False, "reason": "invalid-receipt-match",
                    "item": None}
        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, item_id)
            if rec is None:
                return {"applied": False, "reason": "missing", "item": None}
            if not _is_int_not_bool(expected_receipt_revision) \
                    or rec["receipt_revision"] != expected_receipt_revision:
                return {"applied": False, "reason": "receipt-revision-changed",
                        "item": _snapshot(rec)}
            changed = False
            if receipt_match == "unique":
                if not _nk(external_ref):
                    return {"applied": False,
                            "reason": "missing-external-ref",
                            "item": _snapshot(rec)}
                if rec["status"] == STATUS_ACCEPTED and rec.get("external_ref"):
                    # 已 accepted 且有证据：后写不得覆盖既有受理证据。
                    rec["receipt_match"] = "unique"
                    rec["receipt_revision"] += 1
                    rec["updated_at"] = now
                    _store(self._conn, rec)
                    return {"applied": True, "changed": False,
                            "reason": "already-accepted",
                            "item": _snapshot(rec)}
                rec["status"] = STATUS_ACCEPTED
                rec["external_ref"] = external_ref
                rec["receipt_match"] = "unique"
                rec["claim_owner"] = None
                rec["claim_at"] = None
                rec["revision"] += 1
                changed = True
            else:
                rec["receipt_match"] = receipt_match
                changed = True
            rec["receipt_revision"] += 1
            rec["updated_at"] = now
            _store(self._conn, rec)
            return {"applied": True, "changed": changed,
                    "reason": "receipt-recorded", "item": _snapshot(rec)}


__all__ = [
    "MAIL_SCHEMA", "MAIL_SCHEMA_KEY", "MAIL_KEY_PREFIX",
    "SOURCE_KIND_RESPONSIBILITY", "SOURCE_KIND_TASK", "SOURCE_KIND_INCIDENT",
    "SOURCE_KINDS", "SOURCE_BINDING_KEYS", "SOURCE_BINDING_REQUIRED_KEYS",
    "SOURCE_BINDING_OPTIONAL_KEYS",
    "MailOutboxStateError", "EmailOutbox",
]

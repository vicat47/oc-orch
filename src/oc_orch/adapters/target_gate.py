"""目标级额度闸门的 SQLite/meta 实现（S3.2/S3.3 安全补修 round4 收尾版）。

只复用 ``meta`` 表与 :func:`oc_orch.adapters.sqlite_db.transaction` 短事务，
不新建表；状态以目标 session 为最小作用域持久化在 ``target-gate:{session_id}``
单条 JSON 记录，schema 版本记录在 ``target-gate:schema`` 键。所有方法不做网络。

安全不变量（round4 收尾，E 审查末节确认 round3 阻断闭合）：

- 持久记录在每次 `_load` 后统一验证（见 `_validate_record`）；**任何坏字段一律抛
  :class:`TargetGateStateError`（fail-closed），5 方法在事务内拒绝并回滚，不改
  记录、不填默认放行**。所有写入统一走 `_store`（写入前再次 `_validate_record`），
  保证成功写入的记录必然可再读，不留 invalid-state 落盘。
- deadline 一律 `_bound_deadline` 封顶到 `MAX_CLOCK_MS`：界内 origin_at + delay
  不会算出超过边界的 blocked_until，避免“写入成功但读取自校验拒绝”的故障态。
- 写参数前置守卫：`evidence_key`（observe_error/record_success）与
  `owner/message_id`（claim_probe）必须为非空字符串；坏键返回
  `invalid-evidence-key` / `invalid-probe-ids`（不清门、不写、不消耗预算、不裸
  TypeError），签名不变。
- 时间边界：有效事实时间（origin_at 或回退 observed_at）限
  ``0 <= t <= MAX_CLOCK_MS``；荒谬值（如 10**30）返回 ``invalid-timestamp``，
  不造天文 deadline，不清现有闸门。
- 证据秩序：`recent_errors` 有界集合（RECENT_ERROR_LIMIT）按 key 去重；
  `max_origin_at` 水位线记录已接受的最大规范起源时间；事实判定统一用
  ``origin_at``（稳定源时间，C 用 canonical 执行输入），观察时间只作展示——
  被淘汰的旧事实即使带新观察时间也不能冒充新失败。
- 当前 probe 归属失败：仅 ``probe-fail:{probe_message_id}`` 可安全结束该 probe
  进入下一冷却或第 3 次耗尽；缺失归属的新错误保留原 ID 转 blocked 诊断。
- ``record_success`` 只在 message 非空字符串、确为当前半开原 ID、时间有效且
  ``>= probe_started_at``、晚于新错误水位且查询版本仍匹配时原子清除并审计。
- ``probe_observation`` 是 schema1 的可选增量字段组：旧记录可缺少，存在时
  严格验证；只读补证的预算/期限跨重启保存，不赋予新 probe 或重投权限。
- ``block_manual`` 人工中断优先：blocked_until=None，本组件不自动解锁；只有
  显式 ``clear_manual`` 可以清除 manual 记录，provider 冷却/探测态不会被它清除。
  runtime 对 ``coordinator-interrupted`` 可在用户重新输入后调用该显式入口；
  ``coordinator-paused`` 仍须 ``coordinator unlock``。
- 2026-11-19 用户授权新增显式 ``clear_provider``：operator 可从 CLI/Web 控制面
  对 provider 闸门“恢复调度”。它不是删除记录，而是把阻塞态复位为
  ``probe-ready``：保留 ``source=provider`` 与 ``recent_errors``/``max_origin_at``/
  ``failure_count`` 证据守卫（并移除已失效的 ``probe_observation``，避免与原 probe
  归属不一致的非法态）；新错误仍由 ``observe_error`` 重新登记并冷却，
  fail-closed 不变量不变（不是持久绕过）。manual 闸门仍走 ``clear_manual``。
"""

from __future__ import annotations

import json
import math
import sqlite3
from typing import Any

from ..domain.cooldown import (
    MAX_HALF_OPEN_PROBES,
    cooldown_delay_ms,
)
from .sqlite_db import meta_get, meta_set, transaction

GATE_SCHEMA = 1
GATE_SCHEMA_KEY = "target-gate:schema"
GATE_KEY_PREFIX = "target-gate:"

# 有效时钟边界（毫秒）：拒绝荒谬时间，避免天文 deadline。
MAX_CLOCK_MS = 8 * 10 ** 12  # ~ 2223 年，拒绝 10**30 类垃圾

# 半开探测未决超时：超时仍无归因结果时保留原 message、转可见阻塞（初始拟议值）。
HALF_OPEN_TIMEOUT_MS = 10 * 60 * 1000
# 当前半开探测失败的可归属证据前缀：coordinator 必须以
# ``probe-fail:{probe_message_id}`` 作为 evidence_key 上报当前探测的明确失败。
PROBE_FAILURE_KEY_PREFIX = "probe-fail:"
# 有界证据去重集合容量：避免旧 A/B 证据来回被当作新 failure / 无限推迟。
RECENT_ERROR_LIMIT = 16
# 原 probe 的只读补证预算，与会触发模型执行的半开探测预算分开。
# 同一结束事实最多主动补查 8 次、间隔 30s、窗口自 probe 发出起 1h；
# 新的可信结束事实可核验一次，但不会重置已有次数/期限。
PROBE_OBSERVATION_MAX_CHECKS = 8
PROBE_OBSERVATION_INTERVAL_MS = 30 * 1000
PROBE_OBSERVATION_WINDOW_MS = 60 * 60 * 1000

_SOURCES = frozenset({"provider", "manual"})

# 必需控制字段（round2 起写入的记录必须全部存在且类型正确）。
_REQUIRED_FIELDS = (
    ("blocked_until", "int", True),
    ("category", "str", False),
    ("source", "enum", False),
    ("revision", "int", False),
    ("failure_count", "int", False),
    ("probe_count", "int", False),
    ("probe_message_id", "str", True),
    ("probe_owner", "str", True),
    ("probe_started_at", "int", True),
    ("probe_unknown_since", "int", True),
    ("last_error_key", "str", True),
    ("last_error_at", "int", True),
    ("exhausted", "bool", False),
    ("recent_errors", "dict", True),
)
_INT_MS_FIELDS = ("blocked_until", "last_error_at", "probe_started_at",
                  "probe_unknown_since")
# round4 新增水位线字段（round3 起写入）：缺省 0 兼容旧 round2 记录。
_OPTIONAL_INT_FIELDS = {"max_origin_at": 0}

_MISSING = object()


def _is_int_not_bool(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _valid_ms(value: Any) -> int | None:
    """非负纯数值（含数字字符串/整数 float），且在有效时钟边界内。"""
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


def _is_nk_str(value: Any) -> bool:
    """非空字符串。"""
    return isinstance(value, str) and bool(value.strip())


def _gate_key(session_id: str) -> str:
    return f"{GATE_KEY_PREFIX}{session_id}"


def _bound_deadline(fact_at: int, delay_ms: int) -> int:
    """deadline 封顶到有效时钟边界，保证写入后可读。"""
    return min(MAX_CLOCK_MS, fact_at + delay_ms)


def _validate_record(rec: dict, session_id: str) -> None:
    """持久记录结构性验证；任何坏字段抛 TargetGateStateError（不填默认放行）。"""

    def bad(msg: str):
        raise TargetGateStateError(
            f"target-gate 记录非法（{session_id}）：{msg}")

    if rec.get("schema") != GATE_SCHEMA:
        bad(f"schema={rec.get('schema')!r}")
    if rec.get("session_id") != session_id:
        bad(f"session_id 不匹配 {rec.get('session_id')!r}")
    for key, kind, nullable in _REQUIRED_FIELDS:
        v = rec.get(key, _MISSING)
        if v is _MISSING:
            bad(f"缺少必需控制字段 {key}")
        if v is None and nullable:
            continue
        if key == "source":
            if v not in _SOURCES:
                bad(f"source={v!r}")
        elif kind == "int":
            if not _is_int_not_bool(v) or v < 0:
                bad(f"{key} 非非负整数（bool/文本/float 非法）：{v!r}")
        elif kind == "str":
            if not isinstance(v, str):
                bad(f"{key} 非 str：{v!r}")
        elif kind == "bool":
            if not isinstance(v, bool):
                bad(f"{key} 非 bool：{v!r}")
        elif kind == "dict":
            if not isinstance(v, dict):
                bad(f"{key} 非 dict：{v!r}")
    for key in _INT_MS_FIELDS:
        v = rec.get(key)
        if v is None:
            continue
        if not _is_int_not_bool(v) or not (0 <= v <= MAX_CLOCK_MS):
            bad(f"{key} 超出有效时钟边界：{v!r}")
    for key, default in _OPTIONAL_INT_FIELDS.items():
        v = rec.get(key, default)
        if not _is_int_not_bool(v) or v < 0:
            bad(f"{key} 非非负整数：{v!r}")
    pid, started, owner = (rec.get("probe_message_id"),
                           rec.get("probe_started_at"),
                           rec.get("probe_owner"))
    if pid is None:
        if started is not None or owner is not None:
            bad("probe 关联字段不一致：message 缺失但 started/owner 存在")
    else:
        if started is None or not (isinstance(owner, str) and owner):
            bad("probe 关联字段不一致：message 存在但 started/owner 缺失")
        if not _is_int_not_bool(started) or not (0 <= started <= MAX_CLOCK_MS):
            bad("probe_started_at 超出有效时钟边界")
    for key in ("reason", "message"):
        v = rec.get(key)
        if v is not None and not isinstance(v, str):
            bad(f"展示字段 {key} 非 str/None：{v!r}")
    if "probe_observation" in rec:
        obs = rec["probe_observation"]
        if (not isinstance(obs, dict) or not _is_int_not_bool(obs.get("schema"))
                or obs["schema"] != 1):
            bad("probe_observation schema 非法")
        if (obs.get("message_id") != pid or pid is None
                or not _is_int_not_bool(obs.get("started_at"))
                or obs.get("started_at") != started):
            bad("probe_observation 与原 probe 归属不一致")
        for key in ("checks", "max_checks", "interval_ms", "deadline_at",
                    "next_check_at", "last_idle_at", "last_claim_at"):
            value = obs.get(key)
            if not _is_int_not_bool(value) or not (0 <= value <= MAX_CLOCK_MS):
                bad(f"probe_observation.{key} 非法")
        if (not 1 <= obs["max_checks"] <= PROBE_OBSERVATION_MAX_CHECKS
                or obs["checks"] < 1
                or obs["interval_ms"] != PROBE_OBSERVATION_INTERVAL_MS
                or obs["deadline_at"] != _bound_deadline(
                    started, PROBE_OBSERVATION_WINDOW_MS)
                or not started <= obs["last_idle_at"] <= obs["last_claim_at"]
                or obs["next_check_at"] < obs["last_claim_at"]):
            bad("probe_observation 预算/期限关系非法")


def _load(conn: sqlite3.Connection, session_id: str) -> dict | None:
    """读取单目标闸门记录并验证；损坏/坏字段一律抛稳定错误，绝不 fail-open。"""
    raw = meta_get(conn, _gate_key(session_id))
    if raw is None:
        return None
    try:
        rec = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise TargetGateStateError(
            f"target-gate 记录损坏（JSON）：{session_id}") from exc
    if not isinstance(rec, dict):
        raise TargetGateStateError(
            f"target-gate 记录损坏（非 dict）：{session_id}")
    _validate_record(rec, session_id)
    return rec


def _remember(rec: dict, key: str, seen_at: int) -> None:
    """有界证据去重集合：记录最近见过的 evidence_key → 最近观察时间。"""
    recent = dict(rec.get("recent_errors") or {})
    if key in recent:
        recent[key] = max(recent[key], seen_at)
    else:
        recent[key] = seen_at
        if len(recent) > RECENT_ERROR_LIMIT:
            for old_key in list(recent):
                if old_key != key:
                    del recent[old_key]
                    break
    rec["recent_errors"] = recent


class TargetGateStateError(RuntimeError):
    """目标闸门状态损坏、结构非法或 schema 不兼容。

    调用方必须把该错误视为“状态未知、不得自动发送”：应保留原输入与身份，
    转可见告警/人工处置，而不是当作无闸门放行。
    """


class TargetGate:
    """按目标 session 持久化的额度冷却闸门（最小 API 见 port）。"""

    def __init__(self, conn: sqlite3.Connection, clock: Any):
        self._conn = conn
        self._clock = clock

    # ---- 内部辅助 ----------------------------------------------------
    def _now_ms(self) -> int:
        return int(self._clock.now_ms())

    def _read_schema(self) -> None:
        raw = meta_get(self._conn, GATE_SCHEMA_KEY)
        if raw is None:
            return
        try:
            current = int(raw)
        except (TypeError, ValueError) as exc:
            raise TargetGateStateError(
                f"target-gate schema 记录损坏：{raw!r}") from exc
        if current != GATE_SCHEMA:
            raise TargetGateStateError(
                f"target-gate schema 不兼容：{current} != {GATE_SCHEMA}")

    def _ensure_schema(self) -> None:
        self._read_schema()
        if meta_get(self._conn, GATE_SCHEMA_KEY) is None:
            meta_set(self._conn, GATE_SCHEMA_KEY, str(GATE_SCHEMA))

    def _store(self, rec: dict) -> None:
        """统一写入入口：先过同一 _validate_record 契约，再落盘。

        保证成功写入的记录必然满足读取端验证（不留 invalid-state）。
        """
        _validate_record(rec, rec.get("session_id") or "")
        meta_set(self._conn, _gate_key(rec["session_id"]),
                 json.dumps(rec, ensure_ascii=False))

    @staticmethod
    def _reset_probe(rec: dict) -> None:
        rec["probe_message_id"] = None
        rec["probe_owner"] = None
        rec["probe_started_at"] = None
        rec["probe_unknown_since"] = None
        rec.pop("probe_observation", None)

    @staticmethod
    def _snapshot(rec: dict, now: int) -> dict:
        if rec.get("source") == "manual":
            state = "manual-blocked"
        elif rec.get("exhausted"):
            state = "exhausted"
        elif rec.get("probe_message_id"):
            started = rec.get("probe_started_at")
            if started is not None and now >= started + HALF_OPEN_TIMEOUT_MS:
                state = "probe-unknown"
            else:
                state = "probe-outstanding"
        elif rec.get("blocked_until") is not None and now < rec["blocked_until"]:
            state = "blocked"
        else:
            state = "probe-ready"
        return {
            "session_id": rec.get("session_id"),
            "state": state,
            "blocked_until": rec.get("blocked_until"),
            "category": rec.get("category"),
            "source": rec.get("source"),
            "revision": rec.get("revision", 0),
            "failure_count": rec.get("failure_count", 0),
            "probe_count": rec.get("probe_count", 0),
            "probe_message_id": rec.get("probe_message_id"),
            "probe_owner": rec.get("probe_owner"),
            "probe_started_at": rec.get("probe_started_at"),
            "last_error_key": rec.get("last_error_key"),
            "last_error_at": rec.get("last_error_at"),
            "reason": rec.get("reason"),
            "message": rec.get("message"),
            "exhausted": bool(rec.get("exhausted")),
            "probe_unknown_since": rec.get("probe_unknown_since"),
            "max_half_open_probes": MAX_HALF_OPEN_PROBES,
            "half_open_timeout_ms": HALF_OPEN_TIMEOUT_MS,
            "probe_observation": (dict(rec["probe_observation"])
                                  if "probe_observation" in rec else None),
        }

    def _make_result(self, session_id: str, rec: dict | None, *,
                     changed: bool, reason: str) -> dict:
        return {
            "session_id": session_id,
            "gate": self._snapshot(rec, self._now_ms()) if rec else None,
            "changed": changed,
            "reason": reason,
        }

    # ---- 只读 --------------------------------------------------------
    def get(self, session_id: str) -> dict | None:
        self._read_schema()
        rec = _load(self._conn, session_id)
        if rec is None:
            return None
        return self._snapshot(rec, self._now_ms())

    def list_all(self) -> list[dict]:
        """只读列出全部闸门快照；损坏记录以 ``state=state-error`` 暴露。

        供 Web 控制面展示/发现 manual 闸门，不消耗探测预算，也不放行发送。
        """
        self._read_schema()
        rows = self._conn.execute(
            "SELECT key FROM meta WHERE key LIKE ?",
            (GATE_KEY_PREFIX + "%",)).fetchall()
        out = []
        now = self._now_ms()
        for row in rows:
            key = row["key"]
            if not isinstance(key, str) or not key.startswith(GATE_KEY_PREFIX):
                continue
            session_id = key[len(GATE_KEY_PREFIX):]
            if not session_id or session_id == "schema":
                continue
            try:
                rec = _load(self._conn, session_id)
            except TargetGateStateError as exc:
                out.append({"session_id": session_id, "state": "state-error",
                            "reason": str(exc)[:200]})
                continue
            if rec is not None:
                out.append(self._snapshot(rec, now))
        return out

    # ---- 错误登记 ----------------------------------------------------
    def observe_error(self, session_id: str, classification: dict[str, Any], *,
                      evidence_key: str, observed_at: int,
                      source: str = "provider",
                      origin_at: int | None = None) -> dict:
        """登记错误。origin_at 为兼容追加：证据稳定源时间，缺省回退 observed_at。

        事实判定一律使用 fact_at = origin_at or observed_at；观察时间只作展示。
        evidence_key 必须为非空字符串（坏键不写、返回 invalid-evidence-key）。
        deadline 封顶 MAX_CLOCK_MS，保证写后必可读。
        """
        if not isinstance(classification, dict):
            classification = {}
        category = str(classification.get("category") or "unknown")
        message = str(classification.get("message") or "")[:200]
        retry_after_ms = classification.get("retry_after_ms")
        source = source if source in _SOURCES else "provider"
        obs_ms = _valid_ms(observed_at)
        fact_at = _valid_ms(origin_at) if origin_at is not None else obs_ms
        seen_at = obs_ms if obs_ms is not None else fact_at

        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, session_id)

            if not _is_nk_str(evidence_key):
                return self._make_result(session_id, rec, changed=False,
                                         reason="invalid-evidence-key")
            if fact_at is None:
                return self._make_result(session_id, rec, changed=False,
                                         reason="invalid-timestamp")

            if rec is None:
                delay = cooldown_delay_ms(category, 1, retry_after_ms)
                rec = {
                    "schema": GATE_SCHEMA,
                    "session_id": session_id,
                    "blocked_until": _bound_deadline(fact_at, delay),
                    "category": category,
                    "source": source,
                    "revision": 1,
                    "failure_count": 1,
                    "probe_count": 0,
                    "probe_message_id": None,
                    "probe_owner": None,
                    "probe_started_at": None,
                    "probe_unknown_since": None,
                    "last_error_key": evidence_key,
                    "last_error_at": seen_at or 0,
                    "reason": None,
                    "message": message,
                    "exhausted": False,
                    "recent_errors": {evidence_key: seen_at or 0},
                    "max_origin_at": fact_at,
                }
                self._store(rec)
                return self._make_result(session_id, rec, changed=True,
                                         reason="new-episode")

            if rec.get("source") == "manual":
                return self._make_result(session_id, rec, changed=False,
                                         reason="manual-holds")

            # 1) 有界证据去重：已见证据（含旧 A/B 来回）不得当作新 failure。
            recent = rec.get("recent_errors") or {}
            if evidence_key in recent:
                return self._make_result(session_id, rec, changed=False,
                                         reason="duplicate-evidence")

            # 2) origin 水位线：事实时间不新于已接受的最大规范起源 → 迟到旧事实。
            origin_watermark = rec.get("max_origin_at", 0)
            if origin_watermark is not None and fact_at <= origin_watermark:
                return self._make_result(session_id, rec, changed=False,
                                         reason="stale-evidence")

            # 3) 未决半开保护：保留原 ID；缺失归属只转 blocked 诊断。
            probe_id = rec.get("probe_message_id")
            if probe_id:
                started = rec.get("probe_started_at")
                if started is not None and fact_at < started:
                    return self._make_result(session_id, rec, changed=False,
                                             reason="stale-evidence")
                if (isinstance(evidence_key, str)
                        and evidence_key == f"{PROBE_FAILURE_KEY_PREFIX}{probe_id}"):
                    rec["failure_count"] = rec.get("failure_count", 0) + 1
                    rec["revision"] += 1
                    rec["category"] = category
                    rec["last_error_key"] = evidence_key
                    rec["last_error_at"] = max(rec.get("last_error_at") or 0,
                                               seen_at or fact_at)
                    rec["message"] = message
                    rec["max_origin_at"] = fact_at
                    self._reset_probe(rec)
                    _remember(rec, evidence_key, seen_at or 0)
                    if rec["probe_count"] >= MAX_HALF_OPEN_PROBES:
                        rec["exhausted"] = True
                        rec["blocked_until"] = None
                        rec["reason"] = f"probe-budget-exhausted:{category}"
                        self._store(rec)
                        return self._make_result(session_id, rec, changed=True,
                                                 reason="exhausted")
                    delay = cooldown_delay_ms(category, rec["failure_count"],
                                              retry_after_ms)
                    rec["blocked_until"] = _bound_deadline(fact_at, delay)
                    self._store(rec)
                    return self._make_result(session_id, rec, changed=True,
                                             reason="failure-round")
                rec["probe_unknown_since"] = fact_at
                rec["reason"] = "unattributed-error-while-probe"
                rec["last_error_key"] = evidence_key
                rec["last_error_at"] = max(rec.get("last_error_at") or 0,
                                           seen_at or fact_at)
                rec["max_origin_at"] = fact_at
                rec["revision"] += 1
                _remember(rec, evidence_key, seen_at or 0)
                self._store(rec)
                return self._make_result(session_id, rec, changed=True,
                                         reason="probe-unattributed")

            # 4) 无未决 probe：窗内处理（同证据已被去重集合拦截）。
            if rec.get("blocked_until") is not None and fact_at < rec["blocked_until"]:
                rec["last_error_key"] = evidence_key
                rec["last_error_at"] = max(rec.get("last_error_at") or 0,
                                           seen_at or fact_at)
                rec["max_origin_at"] = max(rec.get("max_origin_at", 0), fact_at)
                rec["message"] = message
                rec["revision"] += 1
                _remember(rec, evidence_key, seen_at or 0)
                self._store(rec)
                return self._make_result(session_id, rec, changed=True,
                                         reason="in-window-no-extension")

            # 5) 窗口已过：同证据兜底（recent 集合已拦截）。
            if rec.get("last_error_key") == evidence_key:
                return self._make_result(session_id, rec, changed=False,
                                         reason="duplicate-evidence")

            # 6) 探测预算耗尽 → 保留责任并转告警（不再自动探测）。
            if rec.get("probe_count", 0) >= MAX_HALF_OPEN_PROBES:
                rec["exhausted"] = True
                rec["blocked_until"] = None
                rec["reason"] = f"probe-budget-exhausted:{category}"
                rec["failure_count"] = rec.get("failure_count", 0) + 1
                rec["revision"] += 1
                rec["last_error_key"] = evidence_key
                rec["last_error_at"] = seen_at or 0
                rec["max_origin_at"] = max(rec.get("max_origin_at", 0), fact_at)
                rec["message"] = message
                self._reset_probe(rec)
                _remember(rec, evidence_key, seen_at or 0)
                self._store(rec)
                return self._make_result(session_id, rec, changed=True,
                                         reason="exhausted")

            # 7) 新失败回合：推进阶梯并按可信 Retry-After/类别重新计算（封顶）。
            failure = rec.get("failure_count", 0) + 1
            delay = cooldown_delay_ms(category, failure, retry_after_ms)
            rec["failure_count"] = failure
            rec["blocked_until"] = _bound_deadline(fact_at, delay)
            rec["category"] = category
            rec["message"] = message
            rec["last_error_key"] = evidence_key
            rec["last_error_at"] = seen_at or 0
            rec["max_origin_at"] = max(rec.get("max_origin_at", 0), fact_at)
            rec["revision"] += 1
            self._reset_probe(rec)
            _remember(rec, evidence_key, seen_at or 0)
            self._store(rec)
            return self._make_result(session_id, rec, changed=True,
                                     reason="failure-round")

    # ---- 半开探测（必须真实发送前原子占用） -------------------------
    def claim_probe(self, session_id: str, *, owner: str,
                    message_id: str) -> dict:
        """单操作原子占用半开探测（公开 API；行为契约保持不变）。

        自管短事务：``_ensure_schema`` + :meth:`_claim_probe_in_tx`。组合适配器
        （wake-admission）为把探针花费与输入 possible 标记做成同一原子提交/回滚，
        在更大事务内直接调用 :meth:`_claim_probe_in_tx`。
        """
        if not (_is_nk_str(owner) and _is_nk_str(message_id)):
            return {"session_id": session_id, "allowed": False,
                    "reason": "invalid-probe-ids", "spent": False, "state": None}
        now = self._now_ms()
        with transaction(self._conn):
            self._ensure_schema()
            return self._claim_probe_in_tx(
                session_id, owner=owner, message_id=message_id, now=now)

    def _claim_probe_in_tx(self, session_id: str, *, owner: str,
                           message_id: str, now: int) -> dict:
        """已开启事务内的窄能力（要求同一 connection，不自行 BEGIN/COMMIT）。

        校验、状态判定与返回形状与原 ``claim_probe`` **逐字一致**；调用方必须
        已 ``BEGIN IMMEDIATE``。拒绝路径保留只读/超时诊断（如
        ``probe_unknown_since`` 水位）；若组合流程需要回滚探针花费，由外层回滚
        整笔事务。损坏记录抛 :class:`TargetGateStateError`（fail-closed）。
        """
        if not self._conn.in_transaction:
            raise RuntimeError("_claim_probe_in_tx需要已开启的短事务")
        rec = _load(self._conn, session_id)
        if rec is None:
            return {"session_id": session_id, "allowed": True,
                    "reason": "idle", "spent": False, "state": None}

        state = self._snapshot(rec, now)
        if rec.get("source") == "manual":
            return {"session_id": session_id, "allowed": False,
                    "reason": "manual", "spent": False, "state": state}
        if rec.get("exhausted"):
            return {"session_id": session_id, "allowed": False,
                    "reason": "exhausted", "spent": False, "state": state}
        if rec.get("blocked_until") is not None and now < rec["blocked_until"]:
            return {"session_id": session_id, "allowed": False,
                    "reason": "cooldown", "spent": False, "state": state}

        if rec.get("probe_message_id") is not None:
            started = rec.get("probe_started_at")
            if started is not None and now >= started + HALF_OPEN_TIMEOUT_MS:
                if rec.get("probe_unknown_since") is None:
                    rec["probe_unknown_since"] = now
                    rec["revision"] += 1
                    self._store(rec)
                    state = self._snapshot(rec, now)
                return {"session_id": session_id, "allowed": False,
                        "reason": "probe-unknown", "spent": False,
                        "state": state}
            return {"session_id": session_id, "allowed": False,
                    "reason": "probe-outstanding", "spent": False,
                    "state": state}

        if rec.get("probe_count", 0) >= MAX_HALF_OPEN_PROBES:
            rec["exhausted"] = True
            rec["blocked_until"] = None
            rec["reason"] = "probe-budget-exhausted"
            rec["revision"] += 1
            self._store(rec)
            state = self._snapshot(rec, now)
            return {"session_id": session_id, "allowed": False,
                    "reason": "exhausted", "spent": False, "state": state}

        rec["probe_count"] = rec.get("probe_count", 0) + 1
        rec["probe_message_id"] = message_id
        rec["probe_owner"] = owner
        rec["probe_started_at"] = now
        rec["probe_unknown_since"] = None
        rec.pop("probe_observation", None)
        rec["blocked_until"] = None  # 探测中不再保持冷却窗口
        rec["revision"] += 1
        self._store(rec)
        return {"session_id": session_id, "allowed": True,
                "reason": "probe", "spent": True,
                "state": self._snapshot(rec, now)}

    # ---- 原 probe 的只读补证预算 -------------------------------------
    def claim_probe_observation(self, session_id: str, *, message_id: str,
                                expected_revision: int, idle_at: int) -> dict:
        """网络查询前原子消耗一次观察预算；不是新的 probe/发送权限。

        旧 schema1 记录可缺少整个可选组；首次使用才惰性登记。半开超时不
        释放原身份，查询失败/进程崩溃也不返还预算。严格验证后以 revision
        仲裁多个观察者；相同事实的周期查询受次数、期限和间隔三重限制。
        """
        now = self._now_ms()
        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, session_id)

            def result(allowed, reason):
                return {"allowed": allowed, "reason": reason,
                        "gate": self._snapshot(rec, now) if rec else None}

            if rec is None:
                return result(False, "no-active-gate")
            if (not _is_int_not_bool(expected_revision)
                    or expected_revision < 1):
                return result(False, "invalid-expected-revision")
            if rec["revision"] != expected_revision:
                return result(False, "revision-changed")
            if rec["source"] == "manual":
                return result(False, "manual-holds")
            if (not _is_nk_str(message_id)
                    or rec["probe_message_id"] != message_id):
                return result(False, "message-mismatch")
            started = rec["probe_started_at"]
            if (not _is_int_not_bool(idle_at)
                    or not started <= idle_at <= now):
                return result(False, "invalid-idle-at")
            obs = rec.get("probe_observation")
            if obs is None:
                obs = {
                    "schema": 1, "message_id": message_id, "started_at": started,
                    "checks": 0, "max_checks": PROBE_OBSERVATION_MAX_CHECKS,
                    "interval_ms": PROBE_OBSERVATION_INTERVAL_MS,
                    "deadline_at": _bound_deadline(started, PROBE_OBSERVATION_WINDOW_MS),
                    "next_check_at": 0, "last_idle_at": None, "last_claim_at": now,
                }
            previous_idle = obs["last_idle_at"]
            if previous_idle is not None and idle_at < previous_idle:
                return result(False, "stale-observation")
            new_fact = previous_idle is None or idle_at > previous_idle
            if not new_fact:
                if (obs["checks"] >= obs["max_checks"]
                        or now >= obs["deadline_at"]):
                    return result(False, "observation-exhausted")
                if now < obs["next_check_at"]:
                    return result(False, "observation-backoff")
            obs = dict(obs, checks=obs["checks"] + 1, last_idle_at=idle_at,
                       last_claim_at=now,
                       next_check_at=_bound_deadline(now, obs["interval_ms"]))
            rec["probe_observation"] = obs
            rec["revision"] += 1
            self._store(rec)
            return result(True, "new-terminal-fact" if new_fact else "observation")

    # ---- 成功清除 ----------------------------------------------------
    def record_success(self, session_id: str, *, message_id: str,
                       observed_at: int, evidence_key: str,
                       expected_revision: int | None = None) -> dict:
        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, session_id)
            if rec is None:
                return {"session_id": session_id, "cleared": False,
                        "reason": "no-active-gate", "evidence_key": evidence_key,
                        "gate": None}
            if not _is_nk_str(evidence_key):
                return {"session_id": session_id, "cleared": False,
                        "reason": "invalid-evidence-key",
                        "evidence_key": evidence_key,
                        "gate": self._snapshot(rec, self._now_ms())}
            if rec.get("source") == "manual":
                return {"session_id": session_id, "cleared": False,
                        "reason": "manual-holds", "evidence_key": evidence_key,
                        "gate": self._snapshot(rec, self._now_ms())}
            if expected_revision is not None:
                if (not _is_int_not_bool(expected_revision)
                        or expected_revision < 1):
                    return {"session_id": session_id, "cleared": False,
                            "reason": "invalid-expected-revision", "evidence_key": evidence_key,
                            "gate": self._snapshot(rec, self._now_ms())}
                if rec["revision"] != expected_revision:
                    return {"session_id": session_id, "cleared": False,
                            "reason": "revision-changed", "evidence_key": evidence_key,
                            "gate": self._snapshot(rec, self._now_ms())}
            if not _is_nk_str(message_id):
                return {"session_id": session_id, "cleared": False,
                        "reason": "invalid-message-id", "evidence_key": evidence_key,
                        "gate": self._snapshot(rec, self._now_ms())}
            probe_id = rec.get("probe_message_id")
            if not (isinstance(probe_id, str) and probe_id):
                return {"session_id": session_id, "cleared": False,
                        "reason": "no-probe", "evidence_key": evidence_key,
                        "gate": self._snapshot(rec, self._now_ms())}
            if message_id != probe_id:
                return {"session_id": session_id, "cleared": False,
                        "reason": "message-mismatch", "evidence_key": evidence_key,
                        "gate": self._snapshot(rec, self._now_ms())}
            at = _valid_ms(observed_at)
            if at is None:
                return {"session_id": session_id, "cleared": False,
                        "reason": "invalid-observed-at", "evidence_key": evidence_key,
                        "gate": self._snapshot(rec, self._now_ms())}
            started = rec.get("probe_started_at")
            if started is not None and at < started:
                return {"session_id": session_id, "cleared": False,
                        "reason": "stale-success", "evidence_key": evidence_key,
                        "gate": self._snapshot(rec, at)}
            watermark = rec.get("max_origin_at", rec.get("last_error_at") or 0)
            if at <= watermark:
                return {"session_id": session_id, "cleared": False,
                        "reason": "newer-error", "evidence_key": evidence_key,
                        "gate": self._snapshot(rec, self._now_ms())}
            # 清门与可追溯证据同事务；崩溃不能留下已删 gate 却无清除依据。
            self._conn.execute(
                "INSERT INTO audit(at, actor, action, entity, entity_id, "
                "from_state, to_state, detail) VALUES(?,?,?,?,?,?,?,?)",
                (self._now_ms(), "target-gate", "target-gate.clear-probe-success",
                 "session", session_id, self._snapshot(rec, self._now_ms())["state"],
                 None, json.dumps({
                     "probe_message_id": probe_id, "probe_started_at": started,
                     "gate_revision": rec["revision"], "success_at": at,
                     "evidence_key": evidence_key,
                     "probe_observation": rec.get("probe_observation"),
                 }, ensure_ascii=False)))
            self._conn.execute(
                "DELETE FROM meta WHERE key=?", (_gate_key(session_id),))
            return {"session_id": session_id, "cleared": True,
                    "reason": "probe-success", "evidence_key": evidence_key,
                    "gate": None}

    # ---- 人工中断 ----------------------------------------------------
    def block_manual(self, session_id: str, *, reason: str) -> dict:
        now = self._now_ms()
        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, session_id)
            if rec is None:
                rec = {
                    "schema": GATE_SCHEMA,
                    "session_id": session_id,
                    "blocked_until": None,
                    "category": "manual",
                    "source": "manual",
                    "revision": 1,
                    "failure_count": 0,
                    "probe_count": 0,
                    "probe_message_id": None,
                    "probe_owner": None,
                    "probe_started_at": None,
                    "probe_unknown_since": None,
                    "last_error_key": None,
                    "last_error_at": now,
                    "reason": str(reason)[:200],
                    "message": "",
                    "exhausted": False,
                    "recent_errors": {},
                    "max_origin_at": 0,
                }
            else:
                rec["source"] = "manual"
                rec["blocked_until"] = None
                rec["reason"] = str(reason)[:200]
                rec["revision"] += 1
                rec["last_error_at"] = now
            self._store(rec)
            return self._make_result(session_id, rec, changed=True,
                                     reason="manual-block")

    def clear_manual(self, session_id: str, *, reason: str = "manual-recovery") -> dict:
        """显式清除人工中断闸门（只接受 ``source=manual``）。

        这是供 CLI/运维显式调用的“解锁”入口，不会被 runtime 自动调用：

        - 无活动闸门 → ``no-active-gate``，不清任何东西；
        - provider 冷却/半开探测/耗尽态 → ``not-manual``，保持原状，避免手滑
          绕过额度保护；
        - manual → 原子删除记录并返回 ``manual-cleared``。

        返回 ``{session_id, cleared, reason, gate}``；记录损坏时抛
        :class:`TargetGateStateError`（fail-closed，不删除）。
        """
        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, session_id)
            if rec is None:
                return {"session_id": session_id, "cleared": False,
                        "reason": "no-active-gate", "gate": None}
            if rec.get("source") != "manual":
                return {"session_id": session_id, "cleared": False,
                        "reason": "not-manual",
                        "gate": self._snapshot(rec, self._now_ms())}
            self._conn.execute(
                "DELETE FROM meta WHERE key=?", (_gate_key(session_id),))
            return {"session_id": session_id, "cleared": True,
                    "reason": "manual-cleared", "gate": None,
                    "note": str(reason)[:200]}

    def clear_provider(self, session_id: str, *,
                       reason: str = "operator-recovery") -> dict:
        """显式恢复 provider 目标调度（2026-11-19 用户授权的 operator 入口）。

        与 ``clear_manual``（删除记录）不同，本方法把 provider 阻塞态原子复位为
        ``probe-ready``，**保留** ``source=provider`` 与 ``recent_errors`` /
        ``max_origin_at`` / ``failure_count`` 证据守卫：随后新的 provider 错误
        仍由 :meth:`observe_error` 重新登记并冷却，fail-closed 不变量不变，
        不构成持久绕过。已失效的 ``probe_observation`` 一并移除（否则与原 probe
        归属不一致，写后不可读）。

        - 无活动闸门 → ``no-active-gate``，不清任何东西；
        - manual（人工中断）→ ``not-provider``，仍须走 ``clear_manual``，互不干扰；
        - provider → 复位 ``blocked_until`` / 未决 probe / 半开未知 / 探测预算
          耗尽标记并把 ``probe_count`` 归零（解除 exhausted 拒绝），状态回到
          ``probe-ready``，返回 ``provider-cleared``。

        返回 ``{session_id, cleared, reason, gate}``；记录损坏时抛
        :class:`TargetGateStateError`（fail-closed，不修改）。
        """
        with transaction(self._conn):
            self._ensure_schema()
            rec = _load(self._conn, session_id)
            if rec is None:
                return {"session_id": session_id, "cleared": False,
                        "reason": "no-active-gate", "gate": None}
            if rec.get("source") != "provider":
                return {"session_id": session_id, "cleared": False,
                        "reason": "not-provider",
                        "gate": self._snapshot(rec, self._now_ms())}
            rec["blocked_until"] = None
            rec["probe_message_id"] = None
            rec["probe_owner"] = None
            rec["probe_started_at"] = None
            rec["probe_unknown_since"] = None
            rec.pop("probe_observation", None)
            rec["exhausted"] = False
            rec["probe_count"] = 0
            rec["revision"] = rec.get("revision", 0) + 1
            rec["reason"] = str(reason)[:200]
            self._store(rec)
            return {"session_id": session_id, "cleared": True,
                    "reason": "provider-cleared",
                    "gate": self._snapshot(rec, self._now_ms()),
                    "note": str(reason)[:200]}

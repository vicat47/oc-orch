"""收集用例（application 层）：事务外证据采集 + 单事务正式提交。

职责边界（见 OpenSpec fix-attempt-completion-integrity design D7/D8/D9）：
- 本模块只做编排与业务判定；文件/消息 I/O 通过注入的 io 端口
  （adapters/results 绑定），台账经 ledger 端口（sqlite_ledger 实现）；
- 正式结果 = 确认的成功终态 + 本轮实质证据（非空 assistant 回报或
  ≥1 个 new/changed 交付物）；partial 只产出诊断快照；
- N2/F7：collection guard 在任何采证 I/O（消息/文件/REST 补证）之前读取，
  提交时 CAS 复核，过时的不同版本不得覆盖后来提交的有效版本；
- F3：消息窗口经 io.read_message_window 有界分页读取（归属区间 + 输入
  锚点），覆盖不足/读取失败绝不形成正式结果，已有有效版本保持不变；
- F6：交付物在事务外只读扫描（check_deliverables persist=False），候选
  行经 commit_collection(prepared_deliverables=…) 与结果/状态/通知同一
  短事务原子提交，回滚不留混合证据；
- F2：legacy 坏终态不直接信任；必须补证后来真实成功区间（REST outcome/
  idle + 非 active + 完整输入锚点），并把恢复证据持久化为
  collection.recovery_execution；原坏 turn/terminal_at/旧 ACK 保持不变；
- 任务 6.2：legacy 同内容通知桥接仅在严格证据匹配（非空、语义内容相同、
  旧通知对应同一结果）时复用旧通知身份；不明旧空结果不压制新版本；
- 补收集预算持久化在 metadata.collection；collect 不隐式 ACK。

Supervisor（自动/补收集）、CLI/Web（人工 collect）经组合根进入本用例。
"""

from __future__ import annotations

import json
from typing import Any, Callable, Protocol

from ..domain import results as rules
from ..domain.errors import LedgerError

RECOVERY_SCHEMA = "oc-orch.collection.recovery.v1"


class CollectionLedger(Protocol):
    def get_attempt(self, attempt_id: str) -> dict: ...
    def get_task(self, task_id: str) -> dict: ...
    def deliverables(self, attempt_id: str) -> list[dict]: ...
    def check_deliverables(self, attempt_id: str, *,
                           persist: bool = True) -> list[dict]: ...
    def list_attempts(self, *, task_id: str | None = None, limit: int = 100) -> list[dict]: ...
    def collection_guard(self, attempt_id: str) -> dict: ...
    def commit_collection(self, attempt_id: str, **fact) -> dict: ...
    def record_collection_pending(self, attempt_id: str, **budget) -> dict: ...
    def carry_records(self, attempt_id: str) -> list[dict]: ...
    def list_notifications(self, *, target_session: str | None = None,
                           state: str | None = None, limit: int = 100) -> list[dict]: ...
    def legacy_result_notification(self, attempt_id: str) -> dict | None: ...
    def audit(self, actor: str, action: str, entity: str, entity_id: str,
              detail: Any = None) -> None: ...
    def add_alert(self, **alert) -> dict | None: ...
    def enqueue_notification(self, **spec) -> dict: ...
    def coordinator_lease(self, project: str) -> dict | None: ...


class CollectionIO(Protocol):
    """窄 I/O 端口（adapters/results 绑定，无业务判定）。"""

    def extract_assistant_text(self, messages: list[dict],
                               since_ms: int) -> tuple[str, list[dict], int]: ...

    def load_baseline(self, ledger, attempt_id: str) -> dict | None: ...

    def compare_provenance(self, ledger, attempt_id: str, *, baseline: dict | None,
                           current: list[dict], carry: list[dict] | None,
                           legacy_rows: list[dict] | None = None,
                           prior_result_items: list[dict] | None = None,
                           budgets: dict | None = None) -> list[dict]: ...

    def publish_version_snapshot(self, ledger, attempt_id: str,
                                 result: dict) -> tuple[str, int, bool]: ...

    def update_compat_view(self, ledger, attempt_id: str, result: dict) -> str: ...

    def write_partial_snapshot(self, ledger, attempt_id: str,
                               data: dict) -> str: ...

    def read_anchor(self, client, session_id: str,
                    message_id: str) -> dict | None: ...

    def read_message_window(self, client, session_id: str, *, max_pages: int,
                            page_size: int, anchor_id: str | None = None,
                            since_ms: int | None = None,
                            until_ms: int | None = None) -> tuple[list[dict], dict]: ...

    def read_legacy_result_snapshot(self, path: str) -> dict | None: ...


def _int_cfg(cfg: dict, key: str, default: int) -> int:
    try:
        v = int(cfg.get(key, default))
        return v if v > 0 else default
    except (TypeError, ValueError):
        return default


def _terminal_of(att: dict) -> dict:
    """已确认终态：仅新协议 execution.terminal（D3 窄事务写入）。

    legacy 台账 terminal_at/terminal_kind 不在此直接信任（F2）：由
    _recovery_evidence 在采证阶段补证真实成功区间后另行构造恢复终态。
    """
    terminal = (att["metadata"] or {}).get("execution", {}).get("terminal") or {}
    if terminal.get("outcome"):
        return terminal
    return {}


def _legacy_terminal_fields(att: dict) -> dict:
    """legacy 台账终态字段（可能为旧实现假终态；仅作恢复引用，不当证据）。"""
    if att.get("terminal_at") and att.get("terminal_kind"):
        return {"at": att["terminal_at"], "kind": att["terminal_kind"]}
    return {}


def _inbox_has_input(client, session_id: str, message_id: str) -> bool:
    """原输入是否仍在 pending inbox（未投递执行）。无 inbox 能力按未知处理。"""
    inbox_fn = getattr(client, "inbox", None)
    if inbox_fn is None:
        return True
    try:
        items = inbox_fn(session_id) or []
    except Exception:
        return True  # 查询失败 = 未知，按“可能未投递”保守处理
    return any(m.get("id") == message_id for m in items)


def _recovery_evidence(client, att: dict, io: CollectionIO,
                       clock: Callable[[], int], cfg: dict) -> dict | None:
    """F2：legacy 坏终态的恢复补证。

    必须同时满足（缺一即不可证明，不得收集）：
    - REST session 事实：outcome=succeeded 且 idle 为有效正整数；
    - 会话当前不 active（运行中的进度文本不是完成证据）；active 查询缺失
      按未知处理（不放行）；
    - 完成区间顺序：time.busy 若存在必须 <= idle（不能把结束后又忙的
      session 归为完成）；
    - 输入身份（两种合法形态）：
      a) dispatch_msg_id 存在且 mode != attach：锚点消息必须 type=user、
         time 为 dict、created 有效，且不在 pending inbox；锚点时间 < 完成；
      b) attach/no_prompt（A/T15，mode == "attach"，无伪 message_id）：已封
         存执行边界的 epoch.started_at 存在且 < 真实完成时间。

    返回恢复证据（含真实区间与原损坏记录引用）；不可证明返回 None。
    """
    session_id = att.get("session_id")
    msg_id = att.get("dispatch_msg_id")
    execution = (att.get("metadata") or {}).get("execution") or {}
    epoch_started = (execution.get("epoch") or {}).get("started_at")
    # 仅 mode == "attach" 才允许无 message_id 的 attach epoch 起界；
    # 普通 legacy 尝试必须有可验证的输入锚点（A/T15 契约，不因缺 msg_id
    # 就自动视为 attach）。
    attach_mode = execution.get("mode") == "attach"
    if not session_id:
        return None
    if not attach_mode and not msg_id:
        return None
    if attach_mode and not (isinstance(epoch_started, int)
                            and epoch_started > 0):
        return None
    get_session = getattr(client, "get_session", None)
    if get_session is None:
        return None
    try:
        session = get_session(session_id)
    except Exception:
        return None
    if not isinstance(session, dict) or session.get("id") != session_id:
        return None
    if session.get("outcome") != "succeeded":
        return None
    time_ = session.get("time") or {}
    if not isinstance(time_, dict):
        return None
    idle = time_.get("idle")
    if not (isinstance(idle, int) and not isinstance(idle, bool) and idle > 0):
        return None
    busy = time_.get("busy")
    if busy is not None and (type(busy) is not int or busy <= 0 or busy > idle):
        # 完成后又出现更晚 busy：当前会话又开工/异常，不能当本次完成
        return None
    active_fn = getattr(client, "active_sessions", None)
    if active_fn is None:
        return None  # 活动状态未知 = 不放行（未知不能当作已停止）
    try:
        active = active_fn()
    except Exception:
        return None  # 查询失败 = 未知
    if not isinstance(active, dict) or session_id in active:
        return None
    if attach_mode:
        bound, bound_kind = int(epoch_started), "attach-epoch"
    else:
        anchor = io.read_anchor(client, session_id, msg_id)
        if not isinstance(anchor, dict) or anchor.get("id") != msg_id:
            return None
        if anchor.get("type") != "user":
            return None
        if anchor.get("sessionID") not in (None, session_id):
            return None
        anchor_time = anchor.get("time") or {}
        if not isinstance(anchor_time, dict):
            return None
        t = anchor_time.get("created")
        if not (isinstance(t, int) and not isinstance(t, bool) and t > 0):
            return None
        bound, bound_kind = t, "input-anchor"
        if _inbox_has_input(client, session_id, msg_id):
            return None  # 输入未投递：排队不算执行
    if idle < bound:
        return None  # 逆序：完成早于本轮起界，不可归属
    # 原输入至唯一 idle 的完整区间；REST 最新成功不能代替输入归属。
    try:
        messages, coverage = io.read_message_window(
            client, session_id, max_pages=_int_cfg(cfg, "result_scan_max_pages", 20),
            page_size=_int_cfg(cfg, "result_max_messages", 100),
            anchor_id=None if attach_mode else msg_id, since_ms=bound, until_ms=idle)
    except Exception:
        return None
    if (coverage.get("complete") is not True
            or (not attach_mode and coverage.get("anchor_found") is not True)):
        return None
    idles = []
    for message in messages:
        created = _m_time(message)
        if not bound <= created <= idle:
            continue
        if message.get("type") == "user" and message.get("id") != msg_id:
            return None
        if message.get("type") == "idle":
            idles.append(message)
    if (len(idles) != 1 or _m_time(idles[0]) != idle
            or idles[0].get("outcome") != "succeeded"):
        return None
    # 区间读取可能耗时；复核最新活动/快照，避免将后来开工的会话作恢复完成。
    try:
        latest, active = get_session(session_id), active_fn()
    except Exception:
        return None
    if (not isinstance(active, dict) or session_id in active
            or not isinstance(latest, dict) or latest.get("id") != session_id
            or latest.get("outcome") != "succeeded"
            or latest.get("time") != session.get("time")):
        return None
    legacy = _legacy_terminal_fields(att)
    return {
        "schema": RECOVERY_SCHEMA,
        "mode": "legacy-recovery",
        "input_message_id": msg_id,
        "bound_kind": bound_kind,
        "anchor_at": bound,
        "real_end_at": idle,
        "session_outcome": "succeeded",
        "source": "rest-session+unique-idle-interval",
        "idle_message_id": idles[0]["id"],
        "coverage": coverage,
        "observed_at": clock(),
        "legacy_terminal": legacy or None,
        "note": "原坏终态仅作引用保留；本收集使用补证的真实成功区间",
    }


def _recovered_terminal(recovery: dict) -> dict:
    return {"outcome": "succeeded", "end_at": recovery["real_end_at"],
            "source": "legacy-recovery", "event_id": None,
            "recovery": True}


def _command_check_gate(ledger: CollectionLedger, attempt_id: str,
                        deliverables: list[dict],
                        current_fp: str) -> tuple[bool, bool, list[dict]]:
    """T34：按当前 content_fp 核验 command 检查 evidence 绑定。

    对每个 required command 交付读 DB 行 evidence（run_checks 写入的绑定
    指纹），判定：
    - evidence.content_fingerprint != current_fp（或缺失、不可解析）→ 该
      检查属于旧输入/未执行 → 不可用于判新输入（unverified，checks_pending）；
    - 绑定当前 fp 且 exit==0 且未超时 → verified（保留检查，不入 unverified）；
    - 绑定当前 fp 但 exit!=0 或超时 → checks_failed（检查失败，也列入
      unverified 使 ready=false）。

    返回 (checks_failed, checks_pending, unverified_deliverables)。
    """
    db_rows = {r["target"]: r for r in ledger.deliverables(attempt_id)}
    checks_failed = False
    checks_pending = False
    unverified: list[dict] = []
    for d in deliverables:
        if d.get("kind") != "command" or not d.get("required"):
            continue
        target = d["target"]
        row = db_rows.get(target) or {}
        raw = row.get("evidence")
        evidence: Any = None
        if isinstance(raw, str):
            try:
                evidence = json.loads(raw)
            except ValueError:
                evidence = None
        elif isinstance(raw, dict):
            evidence = raw
        bound_fp = (evidence or {}).get("content_fingerprint") if isinstance(
            evidence, dict) else None
        if bound_fp != current_fp:
            # 未执行 / 旧输入检查：新输入变更 → 不可判新输入
            checks_pending = True
            unverified.append(d)
            continue
        exit_code = (evidence or {}).get("exit") if isinstance(evidence, dict) else None
        timed_out = bool((evidence or {}).get("timed_out")) if isinstance(
            evidence, dict) else False
        if isinstance(exit_code, int) and exit_code == 0 and not timed_out:
            continue  # 同指纹检查通过：保留
        checks_failed = True  # 绑定当前 fp 但失败/超时
        unverified.append(d)
    return checks_failed, checks_pending, unverified


def _command_checks_dict(deliverables: list[dict],
                         current_fp: str) -> dict[str, dict]:
    """构造 result_version 的检查门稳定字段（status/exit）。"""
    out: dict[str, dict] = {}
    for d in deliverables:
        if d.get("kind") != "command" or not d.get("required"):
            continue
        raw = d.get("evidence") or {}
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except ValueError:
                raw = {}
        if not isinstance(raw, dict):
            raw = {}
        out[d["target"]] = {"status": "verified" if (
            raw.get("content_fingerprint") == current_fp
            and isinstance(raw.get("exit"), int) and raw.get("exit") == 0
            and not raw.get("timed_out")) else "failed" if (
            raw.get("content_fingerprint") == current_fp
            and (not isinstance(raw.get("exit"), int) or raw.get("exit") != 0
                 or raw.get("timed_out"))) else "unverified",
            "exit": raw.get("exit") if isinstance(raw.get("exit"), int)
            else None}
    return {t: {"status": c["status"], "exit": c["exit"]}
            for t, c in out.items()}


def _source_attempts(ledger: CollectionLedger, att: dict) -> dict[str, dict]:
    """同任务更早代次：carry source 校验输入（D6）。"""
    out: dict[str, dict] = {}
    for a in ledger.list_attempts(task_id=att["task_id"], limit=100):
        if a["id"] == att["id"] or a.get("generation", 0) >= att["generation"]:
            continue
        evidence: dict[str, Any] = {}
        for d in ledger.deliverables(a["id"]):
            ev = d.get("evidence")
            if isinstance(ev, str):
                try:
                    ev = json.loads(ev)
                except ValueError:
                    ev = None
            if isinstance(ev, dict) and isinstance(ev.get("sha256"), str):
                evidence[d["target"]] = ev["sha256"]
        out[a["id"]] = {"generation": a["generation"], "task_id": a["task_id"],
                        "evidence_sha256": evidence,
                        "has_baseline": bool(
                            (a["metadata"] or {}).get("execution", {}).get(
                                "baseline"))}
    return out


def _validate_carry(ledger: CollectionLedger, att: dict,
                    carry_manifest: dict | None) -> list[dict]:
    """校验并合并 carry（清单输入错误 → LedgerError CARRY_*）。"""
    declared = {d["target"] for d in ledger.deliverables(att["id"])}
    merged: list[dict] = [c for c in ledger.carry_records(att["id"])]
    if carry_manifest is not None:
        sources = _source_attempts(ledger, att)
        valid, errors = rules.validate_carry_items(
            carry_manifest, attempt_id=att["id"], declared_targets=declared,
            source_attempts=sources)
        if errors:
            raise LedgerError(
                "CARRY_MANIFEST_INVALID",
                "carry 清单存在输入错误；不默认放行、不修改已有结果",
                {"errors": errors})
        for item in valid:
            src = item.get("source_attempt_id")
            if src is not None:
                src_ev = sources.get(src) or {}
                declared_sha = item["baseline_sha256"]
                if declared_sha not in src_ev.get("evidence_sha256", {}).values():
                    raise LedgerError(
                        "CARRY_SOURCE_EVIDENCE_MISMATCH",
                        f"carry 来源 {src} 的交付证据不含声明摘要 {declared_sha[:12]}…",
                        {"target": item["target"], "source_attempt_id": src})
            merged = [m for m in merged if m["target"] != item["target"]]
            merged.append(item)
    # 持久化声明也要与当前声明目标匹配（旧记录里的未知目标剔除）
    return [m for m in merged if m["target"] in declared]


def _same_semantic_result(old: dict, result: dict) -> bool:
    """任务 6.2 桥接判定：旧 legacy 结果与本次收集语义内容相同。

    非空 + 完整正文一致 + 交付摘要集合一致；缺失来源不证明同一有效结果。
    任何字段缺失/不一致 → 不可证明 → 不桥接。
    """
    if not isinstance(old, dict):
        return False
    old_text = old.get("text")
    if not isinstance(old_text, str) or old.get("text_truncated") or result.get("text_truncated"):
        return False  # 旧截断正文不能仅凭自报摘要证明完整内容
    if old.get("attempt_id") != result.get("attempt_id"):
        return False
    if old.get("task_id") != result.get("task_id"):
        return False
    if old.get("generation") != result.get("generation"):
        return False
    if old.get("session_id") != result.get("session_id"):
        return False
    if old.get("schema") != rules.RESULT_SCHEMA:
        return False
    if old.get("assistant_message_count") != result.get("assistant_message_count"):
        return False
    if old_text != result.get("text") or old.get("text_chars") != len(old_text):
        return False
    if old.get("text_full_sha256") not in (None, rules.sha256_text(old_text)):
        return False
    old_items = old.get("deliverables", [])
    new_items = result.get("deliverables", [])
    if not isinstance(old_items, list) or not isinstance(new_items, list):
        return False
    if not old_text.strip() and not old_items:
        return False
    for d in old_items + new_items:
        if (not isinstance(d, dict) or d.get("provenance") not in
                ("new", "changed", "carried") or
                not (d.get("sha256") or d.get("manifest_sha256"))):
            return False
    old_set = {(d.get("target"), d.get("sha256"), d.get("manifest_sha256"))
               for d in old_items}
    new_set = {(d.get("target"), d.get("sha256"), d.get("manifest_sha256"))
               for d in new_items}
    return old_set == new_set


def _bridge_legacy_notification(ledger: CollectionLedger, io: CollectionIO,
                                att: dict, result: dict) -> tuple[str, dict | None]:
    """legacy 同内容通知桥接（任务 6.2/T36）。返回 (dedupe_key, 桥接信息)。

    仅当：旧协议已收集（无 ref 元数据版本）、旧结果文件可读且非空、语义
    内容与本次收集完全相同、且存在对应同一结果的旧 result_ready 通知时，
    复用旧通知身份（dedupe_key）；ACK 状态/期限由既有通知语义保持不变，
    不复活、不重置。任何不明确情况都返回新版本身份（不压制新结果）。
    """
    default_key = f"result-ready:{att['id']}:{result['result_version']}"
    meta = att.get("metadata") or {}
    previous = meta.get("result") or {}
    if previous.get("schema") == "oc-orch.result.ref.v1":
        if previous.get("version") == result["result_version"]:
            return previous.get("notification_dedupe_key") or default_key, None
        return default_key, None
    if not (att.get("result_collected_at") and att.get("result_path")):
        return default_key, None
    reader = getattr(io, "read_legacy_result_snapshot", None)
    if reader is None:
        return default_key, None  # 读不到旧证据即不可证明
    old = reader(att["result_path"])
    if not _same_semantic_result(old or {}, result):
        return default_key, None
    window = result.get("message_window") or {}
    end = window.get("until")
    collected = old.get("collected_at")
    ledger_collected = att.get("result_collected_at")
    if (old.get("terminal_kind") != "succeeded" or att.get("terminal_kind") != "succeeded"
            or old.get("since") != window.get("since")
            or att.get("terminal_at") != end
            or any(type(t) is not int or t <= 0 for t in (end, collected, ledger_collected))
            or not end <= collected <= ledger_collected):
        return default_key, None  # 早产/不同区间的同文结果不压制真实完成
    old_notif = ledger.legacy_result_notification(att["id"])
    if old_notif is None:
        return default_key, None
    payload = old_notif.get("payload") or {}
    if (old_notif.get("task_id") != att["task_id"]
            or old_notif.get("attempt_id") != att["id"]
            or old_notif.get("kind") != "result_ready"
            or old_notif.get("created_at", 0) < ledger_collected
            or payload.get("result_version")
            or payload.get("classification") != "RESULT_READY"
            or payload.get("missing_deliverables")
            or payload.get("result_path") != att["result_path"]
            or any(payload.get(key) != att.get(key) for key in
                   ("task_id", "session_id"))
            or payload.get("attempt_id") != att["id"]):
        return default_key, None
    bridge = {"bridged_from_notification": old_notif.get("id"),
              "legacy_same_content": True}
    return old_notif.get("dedupe_key") or default_key, bridge


def _m_time(m: dict) -> int:
    t = m.get("time") or {}
    try:
        return int(t.get("created") or 0)
    except (TypeError, ValueError):
        return 0


def collect(attempt_id: str, *, ledger: CollectionLedger, client, cfg: dict,
            io: CollectionIO, clock: Callable[[], int],
            auto: bool = False, partial: bool = False,
            carry_manifest: dict | None = None,
            actor: str | None = None) -> dict:
    """统一收集用例：自动/补收集/人工 collect/显式 carry 共用。"""
    actor = actor or ("supervisor" if auto else "coordinator")
    att = ledger.get_attempt(attempt_id)
    if att["status"] == "accepted":
        return {"ok": True, "attempt_id": attempt_id, "status": "accepted",
                "already_accepted": True, "result_path": att.get("result_path"),
                "result_version": ((att["metadata"] or {}).get("result") or {})
                .get("version"), "notification_ids": [], "partial": False}

    # ---- partial：诊断快照，不碰正式标记/版本/通知 -------------------
    # T23：partial 在正式 collect 状态门前执行——对运行中/未确认的尝试也
    # 只产出诊断路径，不因状态门（active/未终态）提前 NOT_READY。
    if partial:
        return _collect_partial(attempt_id, att=att, ledger=ledger, client=client,
                                cfg=cfg, io=io, clock=clock, auto=auto)

    # ---- 门禁：未到可收集状态不能收集 ------------------------------
    COLLECTABLE = ("terminal_succeeded", "result_collected",
                   "result_missing", "terminal_failed",
                   "terminal_interrupted", "verification_failed",
                   "rejected")
    if att["status"] not in COLLECTABLE:
        raise LedgerError(
            "NOT_READY",
            f"尝试 {attempt_id} 尚未到终态（{att['status']}）；"
            "仅在明确需要时用 --partial")

    terminal = _terminal_of(att)
    recovery: dict | None = None
    # legacy 台账终态字段可能是旧实现假终态（F2）：guard 之后补证真实区间
    attempt_recovery = None if terminal else _legacy_terminal_fields(att)

    # ---- N2/F7：guard 在任何采证 I/O 之前（消息/文件/REST 补证） -----
    guard = ledger.collection_guard(attempt_id)

    if attempt_recovery:
        recovery = _recovery_evidence(client, att, io, clock, cfg)
        if recovery is None:
            raise LedgerError(
                "NOT_READY",
                f"尝试 {attempt_id} 的历史终态缺少可证明的真实成功区间"
                "（legacy 假终态不能直接收集）；需真实 REST 成功事实与"
                "输入锚点补证，或走 --partial 诊断",
                {"legacy_terminal": attempt_recovery,
                 "recovery": "unproven"})
        terminal = _recovered_terminal(recovery)

    if not terminal or terminal.get("outcome") != "succeeded":
        # 失败/中断/未确认：保留诊断，不发布成功结果（D7）
        return _collect_diagnostic(attempt_id, att=att, ledger=ledger,
                                   client=client, cfg=cfg, io=io, clock=clock,
                                   auto=auto, terminal=terminal)

    # ---- 证据采集（事务外 I/O；guard 已在前面取得） -------------------
    now = clock()
    interval_ms = _int_cfg(cfg, "collect_retry_interval", 15) * 1000
    max_attempts = _int_cfg(cfg, "collect_max_attempts", 5)

    carry = _validate_carry(ledger, att, carry_manifest)
    baseline = io.load_baseline(ledger, attempt_id)
    legacy_rows = None if baseline else ledger.deliverables(attempt_id)
    # 已提交版本的 provenance（含 sha）作为 legacy-observation 观察基线（M2）
    prior_result_items = ((att["metadata"] or {}).get("deliverable_provenance")
                          or []) if not baseline else None

    # 消息边界（F3）：本轮输入锚点 → 归属终态；上界始终存在
    session_id = att.get("session_id")
    msg_id = att.get("dispatch_msg_id")
    anchor_at = None
    if session_id and msg_id:
        anchor = io.read_anchor(client, session_id, msg_id)
        if anchor is not None:
            t = ((anchor or {}).get("time") or {}).get("created")
            if isinstance(t, int) and not isinstance(t, bool) and t > 0:
                anchor_at = t
    if anchor_at is None:
        # 起界回退链：已确认终态的 epoch 起点 > 已封存执行边界的 attach
        # epoch（no_prompt 附着捕获的 busy；A/T15 契约：无伪 message_id，
        # 起界取 execution.epoch.started_at）> 台账时间事实。
        exec_epoch_started = ((att["metadata"].get("execution") or {})
                              .get("epoch") or {}).get("started_at")
        anchor_at = (terminal.get("epoch_started_at")
                     or (exec_epoch_started if isinstance(exec_epoch_started, int)
                         and exec_epoch_started > 0 else None)
                     or att.get("started_at") or att.get("dispatched_at")
                     or att.get("created_at") or 0)
    until = terminal.get("end_at") or (
        recovery["real_end_at"] if recovery else None) or att.get("terminal_at")
    if not (isinstance(until, int) and until > 0):
        # 不能无上界收集（F2/T26）：上界缺失即覆盖不足，不形成正式结果
        _coverage_incomplete(ledger, att, actor=actor, reason="no-upper-bound",
                             coverage={"complete": False,
                                       "reason": "no-upper-bound",
                                       "since": anchor_at, "until": None})
    reader = getattr(io, "read_message_window", None)
    if reader is None:
        raise LedgerError(
            "COLLECT_FAILED",
            "收集 I/O 端口缺少有界消息窗口能力（read_message_window）；"
            "不退化为单页读取以免覆盖不足冒充完整")
    try:
        messages, coverage = reader(
            client, session_id,
            max_pages=_int_cfg(cfg, "result_scan_max_pages", 20),
            page_size=_int_cfg(cfg, "result_max_messages", 100),
            anchor_id=(msg_id or None), since_ms=anchor_at or 0,
            until_ms=until)
    except Exception as e:
        _coverage_incomplete(ledger, att, actor=actor,
                             reason=f"message-read-failed:{e!r:.160}",
                             coverage={"complete": False, "error": repr(e)[:200],
                                       "since": anchor_at, "until": until})
    if not isinstance(coverage, dict) or coverage.get("complete") is not True:
        # F3/T25：覆盖不足/读取失败绝不正式提交；已有有效版本保持不变
        _coverage_incomplete(ledger, att, actor=actor,
                             reason="message-window-coverage-incomplete",
                             coverage=coverage if isinstance(coverage, dict)
                             else {"complete": False, "reason": "unknown-coverage"})
    window = [m for m in (messages or [])
              if _m_time(m) >= (anchor_at or 0) and _m_time(m) <= until]
    if msg_id and coverage.get("anchor_found") is not True:
        # 输入锚点未在可见窗口内：输入身份不明（协议降级/截断），
        # 不能把“未出现在当前页”解释为不存在或已完成（T26）。
        _coverage_incomplete(ledger, att, actor=actor,
                             reason="anchor-not-covered",
                             coverage={**coverage, "complete": False,
                                       "reason": "anchor-not-covered"})
    coverage = {**coverage, "anchor": msg_id, "since": anchor_at,
                "until": until, "window_messages": len(window)}
    text, tools, count = io.extract_assistant_text(window, anchor_at or 0)

    # 交付物：物理状态 + 来源维度（先取 legacy 行再扫描，防自我证明）
    # F6/D5：persist=False 只返回候选基础行（不内容哈希/glob 枚举）；严格
    # 扫描（status/sha/manifest/预算/完整性）由 compare_provenance 权威产生
    current = ledger.check_deliverables(attempt_id, persist=False)
    # budgets=cfg 传给适配器做有界共享扫描（跨组接口已一致，不设回退：
    # 真实 TypeError 必须原样暴露，取消预算回退）。
    deliverables = io.compare_provenance(
        ledger, attempt_id, baseline=baseline, current=current, carry=carry,
        legacy_rows=legacy_rows, prior_result_items=prior_result_items,
        budgets=cfg)
    missing = [d for d in deliverables
               if d["required"] and d.get("kind") != "command"
               and d["status"] in ("missing", "expected")]
    unverified = [d for d in deliverables
                  if d["required"] and d["status"] in ("unverified", "manual")
                  and d["kind"] == "command"]
    stale = rules.stale_deliverables(deliverables)
    # D8 同内容视图稳定性：已有提交版本的观察精化（unknown→unchanged 等
    # "非新证据"之间的标签迁移）不改变内容身份；对 sha 未变的项沿用已提交
    # 版本的 provenance 标签，使同内容重收集的快照业务视图与首收集一致
    # （T27/T30：同名版本文件可复用，C 的 MISMATCH 校验不误拒）。
    # carried 是每次重验的显式来源结论，不属于观察标签稳定化：既不能被
    # 旧标签覆盖，也不能在当前 carry 失效时从旧结果恢复。
    if prior_result_items:
        prior_by_target = {p.get("target"): p for p in prior_result_items
                           if isinstance(p, dict)}
        new_evidence = {"new", "changed"}
        for d in deliverables:
            prior = prior_by_target.get(d["target"])
            if not prior:
                continue
            if "carried" in (d.get("provenance"), prior.get("provenance")):
                continue  # carry 有效性始终以本次完整扫描/摘要比较为准
            same_content = (d.get("sha256") == prior.get("sha256")
                            and d.get("manifest_sha256")
                            == prior.get("manifest_sha256"))
            if not same_content:
                continue  # 内容已变：以当前判定为准（新版本）
            if d.get("provenance") in new_evidence:
                # sha 与已提交版本相同却判"新证据"意味着观察基线差异，
                # 保持当前判定（会形成不同版本，交由 CAS/门禁处理）
                continue
            # 内容未变且非新证据：沿用已提交版本标签（unknown→unchanged
            # 等观察精化不制造业务视图差异）
            prior_prov = prior.get("provenance")
            if isinstance(prior_prov, str) and d.get("provenance") != prior_prov:
                d["provenance"] = prior_prov
                d["provenance_detail"] = dict(
                    d.get("provenance_detail") or {},
                    prior_label=prior_prov)
    stale = rules.stale_deliverables(deliverables)
    text_sha = rules.sha256_text(text) if text.strip() else None
    effective = rules.effective_collection(
        assistant_text_sha=text_sha, assistant_count=count,
        deliverables=deliverables)

    if not effective:
        # 空结果/纯工具进度/全部沿用：不占正式标记；有界补收集（D7）
        reason = ("no-substantive-evidence" if not text.strip()
                  else "empty-assistant-text")
        pending = ledger.record_collection_pending(
            attempt_id, reason=reason, next_collect_at=now + interval_ms,
            max_attempts=max_attempts, interval_ms=interval_ms, carry=carry,
            actor=actor)
        if pending.get("exhausted"):
            _notify_result_missing(ledger, att, carry, actor)
        return {
            "ok": True, "attempt_id": attempt_id,
            "status": pending["attempt"]["status"],
            "pending": True, "effective": False,
            "result_path": None, "result_version": None,
            "notification_ids": [],
            "text_chars": len(text), "assistant_message_count": count,
            "missing": missing, "unverified": unverified, "stale": stale,
            "deliverables": deliverables, "coverage": coverage,
            "collection_state": (pending["attempt"]["metadata"].get("collection")
                                 or {}).get("state"),
            "next_collect_at": (pending["attempt"]["metadata"].get("collection")
                                or {}).get("next_collect_at"),
        }

    # ---- 正式结果：指纹 → 不可变快照 → 单事务提交 --------------------
    # D8 指纹稳定性：attempt 输入只取稳定身份字段（id/task/session/generation/
    # 输入消息/已保存 carry）；整份 attempt dict 含 metadata.result/collection
    # 等易变子段，会让同内容的不同收集器算出不同指纹（T30 反例）。
    fp_attempt = {
        "id": att["id"], "task_id": att["task_id"],
        "session_id": att.get("session_id"),
        "generation": att.get("generation"),
        "dispatch_msg_id": att.get("dispatch_msg_id"),
        "carry_records": carry,
    }
    content_fp = rules.content_fingerprint(
        attempt=fp_attempt, execution=terminal,
        message_ids=[m.get("id") for m in window if m.get("id")],
        text_sha256=text_sha or rules.sha256_text(""),
        deliverables=deliverables)
    # ---- T34：command 检查按当前 content_fp 绑定判定（D10/D9 表） ----
    checks_failed, checks_pending, unverified = _command_check_gate(
        ledger, attempt_id, deliverables, current_fp=content_fp)
    version = rules.result_version(
        content_fp=content_fp, missing=[d["target"] for d in missing],
        stale=[d["target"] for d in stale],
        unverified=[d["target"] for d in unverified],
        checks=_command_checks_dict(deliverables, current_fp=content_fp))
    collected_at = clock()  # 单次取时（快照与台账一致）
    result = {
        "schema": rules.RESULT_SCHEMA,
        "protocol_version": rules.PROTOCOL_VERSION,
        "result_version": version,
        "content_fingerprint": content_fp,
        "attempt_id": attempt_id, "task_id": att["task_id"],
        "session_id": session_id, "generation": att["generation"],
        "collected_at": collected_at, "auto": auto,
        "message_window": coverage,
        "assistant_message_count": count, "text_chars": len(text),
        "text_truncated": len(text) > _int_cfg(cfg, "result_store_limit", 200000),
        "text_full_sha256": rules.sha256_text(text),
        "text": text[:_int_cfg(cfg, "result_store_limit", 200000)],
        "tools": tools, "terminal_kind": "succeeded",
        "deliverables": [{"target": d["target"], "kind": d["kind"],
                          "status": d["status"],
                          "sha256": d.get("sha256"),
                          "manifest_sha256": d.get("manifest_sha256"),
                          "provenance": d["provenance"],
                          # detail 为诊断文本（reason 随观察路径变化），
                          # 不进入版本快照身份；完整 detail 在 collect 返回值
                          "carry_source": (d.get("provenance_detail") or {}).get(
                              "source_attempt_id")}
                         for d in deliverables],
        "carry": carry,
        "missing_deliverables": [d["target"] for d in missing],
        "stale_deliverables": [d["target"] for d in stale],
        "unverified_deliverables": [d["target"] for d in unverified],
        "collection_state": "complete",
    }
    if recovery is not None:
        result["recovery_execution"] = recovery
    version_path, snapshot_collected_at, existed = io.publish_version_snapshot(
        ledger, attempt_id, result)
    if existed:
        collected_at = snapshot_collected_at  # 并发同指纹：复用原快照时间
        result["collected_at"] = collected_at
    classification, kind = rules.classify_publication(
        effective=True, missing=[d["target"] for d in missing],
        stale=[d["target"] for d in stale],
        unverified=[d["target"] for d in unverified],
        checks_failed=checks_failed, checks_pending=checks_pending)
    ready = rules.ready_for_verification(
        missing=[d["target"] for d in missing], stale=[d["target"] for d in stale],
        unverified=[d["target"] for d in unverified])
    task = guard["task"]
    # 任务 6.2：legacy 同内容通知桥接（严格证据匹配才复用旧通知身份）
    dedupe_key, bridge_info = _bridge_legacy_notification(ledger, io, att, result)
    # 同版本重复收集时 helper 按 dedupe 幂等返回既有通知（预算/ACK 期限不重置）
    notification = _build_notification(
        kind=kind, classification=classification, task=task, att=att,
        version=version, version_path=version_path, missing=missing,
        stale=stale, unverified=unverified, ready=ready,
        dedupe_key=dedupe_key, bridge=bridge_info)
    summary = (text or "").replace("\n", " ")[:700]
    # F6/D5：交付候选行基于 compare_provenance 严格输出（status/sha256/
    # manifest_sha256 权威），prepared = 该 attempt 的 file/dir/glob 候选
    prepared = []
    for d in deliverables:
        if d.get("kind") not in ("file", "dir", "glob"):
            continue
        cand = {"id": d.get("id"), "kind": d.get("kind"),
                "target": d.get("target"), "status": d.get("status")}
        raw_evidence = d.get("evidence") or {}
        if isinstance(raw_evidence, str):
            import json as _json
            try:
                evidence = _json.loads(raw_evidence) if isinstance(raw_evidence, str) else dict(raw_evidence)
            except ValueError:
                evidence = {}
        elif isinstance(raw_evidence, dict):
            evidence = dict(raw_evidence)
        else:
            evidence = {}
        if d.get("sha256"):
            evidence["sha256"] = d["sha256"]
        if d.get("manifest_sha256"):
            evidence["manifest_sha256"] = d["manifest_sha256"]
        if d.get("scan_reason"):
            evidence["scan_reason"] = d["scan_reason"]
        cand["evidence"] = evidence
        prepared.append(cand)
    committed = ledger.commit_collection(
        attempt_id, result_version=version, content_fingerprint=content_fp,
        collected_at=collected_at, version_path=version_path, summary=summary,
        collection_state="complete", attempt_status="result_collected",
        task_status="result_received",
        task_blocked_reason=None,
        carry=carry,
        prepared_deliverables=prepared,
        provenance_summary=[{"target": d["target"], "kind": d["kind"],
                             "required": bool(d["required"]),
                             "provenance": d["provenance"],
                             "sha256": d.get("sha256"),
                             "manifest_sha256": d.get("manifest_sha256")}
                            for d in deliverables],
        execution_boundary=recovery,
        notification=notification, guard=guard, actor=actor,
        reason="auto-collect" if auto else "collect")
    io.update_compat_view(ledger, attempt_id, {**result,
                                               "collected_at": collected_at})
    notification_ids = committed.get("notification_ids") or []
    if notification and committed.get("notification", {}).get(
            "target_session") is None:
        # 无路由告警（route_pending 已落库，投递待补路由；不烧预算）
        ledger.add_alert(
            severity="high", kind="result_unrouted",
            title=f"结果待路由：{att['task_id']}",
            detail=f"新结果版本 {version} 无可用 coordinator；"
                   f"待发布责任已持久化（{version_path}）",
            dedupe_key=f"result-unrouted:{attempt_id}:{version}",
            task_id=att["task_id"], attempt_id=attempt_id)
    return {
        "ok": True, "attempt_id": attempt_id,
        "status": committed["attempt"]["status"],
        "effective": True, "pending": False, "duplicate": committed["duplicate"],
        "result_path": version_path, "result_version": version,
        "notification_ids": notification_ids,
        "classification": classification,
        "collection_state": "complete",
        "ready_for_verification": ready,
        "text_chars": len(text), "assistant_message_count": count,
        "missing": missing, "unverified": unverified, "stale": stale,
        "deliverables": deliverables, "coverage": coverage,
        "recovered": bool(recovery),
        "collected_at": committed["collected_at"],
    }


def _read_window_or_diag(client, session_id: str, *, io: CollectionIO,
                         cfg: dict, since_ms: int,
                         until_ms: int | None) -> tuple[list[dict], dict]:
    """诊断/partial 路径的有界窗口读取：失败降级为 coverage 标记。"""
    reader = getattr(io, "read_message_window", None)
    if reader is None:
        return [], {"complete": False, "reason": "window-io-unavailable"}
    try:
        return reader(client, session_id,
                      max_pages=_int_cfg(cfg, "result_scan_max_pages", 20),
                      page_size=_int_cfg(cfg, "result_max_messages", 100),
                      anchor_id=None, since_ms=since_ms or 0,
                      until_ms=until_ms)
    except Exception as e:
        return [], {"complete": False, "error": repr(e)[:200]}


def _collect_partial(attempt_id: str, *, att: dict, ledger: CollectionLedger,
                     client, cfg: dict, io: CollectionIO,
                     clock: Callable[[], int], auto: bool) -> dict:
    """partial：只读诊断快照；不写正式标记/版本/通知、不隐式 ACK（T23）。"""
    since = att.get("started_at") or att.get("dispatched_at") or 0
    messages, coverage = _read_window_or_diag(
        client, att.get("session_id"), io=io, cfg=cfg, since_ms=since,
        until_ms=None)
    text, tools, count = io.extract_assistant_text(messages or [], since or 0)
    data = {
        "schema": "oc-orch.result.partial.v1", "attempt_id": attempt_id,
        "task_id": att["task_id"], "session_id": att["session_id"],
        "collected_at": clock(), "auto": auto,
        "assistant_message_count": count, "text_chars": len(text),
        "text": text[:_int_cfg(cfg, "result_store_limit", 200000)],
        "tools": tools, "terminal_kind": att.get("terminal_kind"),
        "terminal_at": att.get("terminal_at"),
        "coverage": coverage, "partial": True,
    }
    path = io.write_partial_snapshot(ledger, attempt_id, data)
    return {"ok": True, "attempt_id": attempt_id,
            "status": att["status"], "partial": True,
            "diagnostic_path": path, "result_path": None,
            "result_version": None, "notification_ids": [],
            "text_chars": len(text), "assistant_message_count": count,
            "coverage": coverage}


def _collect_diagnostic(attempt_id: str, *, att: dict, ledger: CollectionLedger,
                        client, cfg: dict, io: CollectionIO,
                        clock: Callable[[], int], auto: bool,
                        terminal: dict) -> dict:
    """失败/中断/未确认终态：诊断快照，不发布成功结果（D7）。"""
    messages, coverage = _read_window_or_diag(
        client, att.get("session_id"), io=io, cfg=cfg,
        since_ms=att.get("started_at") or 0, until_ms=None)
    since = att.get("started_at") or 0
    text, tools, count = io.extract_assistant_text(messages or [], since or 0)
    data = {"schema": "oc-orch.result.partial.v1",
            "attempt_id": attempt_id, "task_id": att["task_id"],
            "session_id": att["session_id"], "collected_at": clock(),
            "auto": auto, "assistant_message_count": count,
            "text_chars": len(text),
            "text": text[:_int_cfg(cfg, "result_store_limit", 200000)],
            "tools": tools, "terminal_kind": att.get("terminal_kind"),
            "partial": True, "diagnostic": True,
            "coverage": coverage}
    path = io.write_partial_snapshot(ledger, attempt_id, data)
    return {"ok": True, "attempt_id": attempt_id, "status": att["status"],
            "partial": True, "diagnostic_path": path, "result_path": None,
            "result_version": None, "notification_ids": [],
            "text_chars": len(text), "assistant_message_count": count,
            "coverage": coverage}


def _coverage_incomplete(ledger: CollectionLedger, att: dict, *, actor: str,
                         reason: str, coverage: dict) -> None:
    """F3/T25：覆盖不足/读取失败 → 不正式提交；已有有效版本保留并留失败责任。"""
    if att.get("result_collected_at") and att.get("result_path"):
        # 已有有效版本：不用失败覆盖；显式留审计（collection.state 不降级）
        try:
            ledger.audit(actor, "collection.coverage-incomplete-kept",
                         "attempt", att["id"],
                         {"reason": reason[:200],
                          "kept_result_path": att["result_path"]})
        except Exception:
            pass
    raise LedgerError(
        "COLLECT_FAILED",
        f"消息窗口覆盖不足/读取失败：{reason[:200]}；"
        "不形成正式结果，已有有效版本保持不变",
        {"coverage": coverage, "reason": reason[:300]})


def _build_notification(*, kind: str, classification: str, task: dict, att: dict,
                        version: str, version_path: str, missing: list,
                        stale: list, unverified: list, ready: bool,
                        dedupe_key: str | None = None,
                        bridge: dict | None = None) -> dict:
    payload = {
        "task_id": task["id"], "attempt_id": att["id"],
        "session_id": att.get("session_id"),
        "result_version": version, "result_path": version_path,
        "collection_state": "complete",
        "classification": classification,
        "missing_deliverables": [d["target"] for d in missing],
        "stale_deliverables": [d["target"] for d in stale],
        "unverified_deliverables": [d["target"] for d in unverified],
        "ready_for_verification": ready,
        "summary": _classification_summary(classification, missing, stale,
                                           unverified),
        "next_action": (
            "读取不可变版本快照并核对交付物；按验收标准 verify；"
            "处理完毕显式 ack 本通知 ID（collect 不再隐式 ACK）"),
    }
    if bridge:
        payload.update(bridge)
    return {
        "kind": kind,
        "target_session": task.get("coordinator_session"),
        "dedupe_key": dedupe_key or f"result-ready:{att['id']}:{version}",
        "payload": payload,
        "project": task["project"], "task_id": task["id"],
        "attempt_id": att["id"], "priority": 6, "max_attempts": 3,
    }


def _classification_summary(classification: str, missing: list, stale: list,
                            unverified: list) -> str:
    if classification == "DELIVERABLE_MISSING":
        return f"结果已收集；缺必需交付物 {len(missing)} 项"
    if classification == "DELIVERABLE_STALE":
        return f"结果已收集；{len(stale)} 项旧产物未声明沿用（stale）"
    if classification == "CHECKS_PENDING":
        return f"结果已收集；{len(unverified)} 项检查未执行/未验证"
    if classification == "CHECKS_FAILED":
        return f"结果已收集；{len(unverified)} 项检查失败，需修复后重新检查"
    return "结果已收集且交付门禁满足；可进入验收"


def _notify_result_missing(ledger: CollectionLedger, att: dict, carry: list,
                           actor: str) -> None:
    """预算耗尽：一次可操作的缺结果通知（幂等 dedupe）。"""
    task = ledger.get_task(att["task_id"])
    if task["status"] in ("accepted", "cancelled"):
        return
    target = task.get("coordinator_session")
    if not target:
        lease = ledger.coordinator_lease(task["project"])
        target = lease["holder_session"] if lease else None
    ledger.enqueue_notification(
        kind="result_missing", target_session=target,
        dedupe_key=f"result-missing:{att['id']}",
        payload={
            "task_id": task["id"], "attempt_id": att["id"],
            "session_id": att.get("session_id"),
            "classification": "RESULT_MISSING",
            "summary": "执行成功但补收集预算耗尽，未取得本轮有效结果",
            "missing_deliverables": [], "stale_deliverables": [],
            "unverified_deliverables": [], "ready_for_verification": False,
            "next_action": "人工读取 session 消息与现场文件；显式 collect 补证或"
                           "verify --fail 记录原因；不要直接 accepted",
        },
        project=task["project"], task_id=task["id"], attempt_id=att["id"],
        priority=7)

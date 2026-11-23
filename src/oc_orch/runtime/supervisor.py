"""Supervisor：事件监听 + REST 对账 + 持久通知 + 受控恢复。

核心原则：
- 事件只是触发器，REST 状态才是对账事实；SSE 断线后必须补对账。
- 执行终态（execution.succeeded/failed/interrupted）≠ 任务验收通过。
- 观察事实与发送唤醒分离；同一去重键只保留一个未消费通知。
- 写入现场按台账串行；未确认停止的旧尝试不释放写权。
"""

from __future__ import annotations

import fcntl
import json
import os
import queue
import re
import subprocess
import time
from typing import Any

from .. import __version__
from ..application.ports import Clock, OpenCodeClient
from ..composition import (
    ADMISSION_PREPARED,
    ADMISSION_POSSIBLE,
    ADMISSION_ADMITTED,
    ADMISSION_DELIVERED,
    ADMISSION_UNKNOWN,
    ADMISSION_CANCELLED,
    ADMISSION_RELEASED,
    PROBE_FAILURE_KEY_PREFIX,
    TargetGateStateError,
    Client,
    Ledger,
    SystemClock,
    collect_attempt,
    init,
    load_config,
    make_managed_inputs,
    make_responsibilities,
    make_target_gate,
    make_coordinator_control,
    make_turn_observation,
    make_registered_turn_observer,
    make_dependency_wait,
    make_wake_admission,
)
from ..domain.classify import (
    classify_event,
    classify_http_error,
    classify_provider_error,
)
from ..domain.errors import ApiError, LedgerError, TransportError
from ..domain import execution as execution_rules
from ..domain import managed_inputs as input_rules
from ..domain.rules import ATTEMPT_TERMINAL
# S2/S3 契约常量与安全异常统一经组合根再导出（runtime 不直接依赖 adapters）。
from ..domain.wake_policy import should_suppress_wake
from ..util import (
    atomic_write,
    ensure_home,
    iso,
    json_dumps,
    json_loads,
    redact,
    short_id,
)
from .stream import EventStream

LOCK_NAME = "supervisor.lock"
PID_NAME = "supervisor.pid"
HEARTBEAT_NAME = "supervisor.heartbeat"
LOG_NAME = "supervisor.log"

# 目标闸门阻塞 push 的状态集（S3）：冷却/人工/耗尽/未决或未知半开均不得
# 追加自动输入，也不消耗通知发送预算。
_GATE_PUSH_BLOCK_STATES = frozenset({
    "blocked", "manual-blocked", "exhausted", "probe-outstanding", "probe-unknown",
})

# S4 结构化接续报告（严格 JSON envelope / metadata.continuation）。
CONTINUATION_SCHEMA = input_rules.CONTINUATION_SCHEMA
CONTINUATION_KINDS = ("completed", "blocked", "needs_material",
                      "needs_decision")
CONTINUATION_CLASSIFICATION = {
    "completed": "COMPLETED_REQUIRES_REVIEW",
    "blocked": "BLOCKED",
    "needs_material": "NEEDS_MATERIAL",
    "needs_decision": "NEEDS_DECISION",
}
_CONTINUATION_BINDING = input_rules.CONTINUATION_BINDING
_CONTINUATION_PAYLOAD_KEYS = ("materials", "missing_materials", "decision",
                              "evidence_refs")
_FENCED_JSON = re.compile(r"```json\s*(\{.*?\})\s*```", re.DOTALL)


def _message_text(msg: dict) -> str:
    """只读取文本 parts；不扫描关键词，不做自然语言判定。"""
    if not isinstance(msg, dict):
        return ""
    parts: list[str] = []
    if isinstance(msg.get("text"), str):
        parts.append(msg["text"])
    content = msg.get("content")
    if isinstance(content, list):
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text" \
                    and isinstance(part.get("text"), str):
                parts.append(part["text"])
    return "\n".join(parts)


def _accept_continuation(report: dict, *, via_metadata: bool) -> dict | None:
    """纯校验：未知 schema/缺 kind/next_action/绑定 → 不认（unknown）。"""
    if not isinstance(report, dict):
        return None
    schema = report.get("schema")
    if not via_metadata and schema != CONTINUATION_SCHEMA:
        return None
    if via_metadata and schema is not None and schema != CONTINUATION_SCHEMA:
        return None
    kind = report.get("kind")
    if kind not in CONTINUATION_KINDS:
        return None
    next_action = report.get("next_action")
    if not isinstance(next_action, str) or not next_action.strip():
        return None
    if any(key not in report for key in _CONTINUATION_BINDING):
        return None
    out = {key: report.get(key) for key in _CONTINUATION_BINDING}
    out.update({"schema": CONTINUATION_SCHEMA, "kind": kind,
                "next_action": next_action.strip()})
    for key in _CONTINUATION_PAYLOAD_KEYS:
        if key in report:
            out[key] = report.get(key)
    return out


def extract_continuation_report(messages, *, after_ms: int,
                                until_ms: int) -> dict:
    """从唯一执行区间的 assistant 消息读取严格报告。

    - 只认整段 JSON 或 ```json fenced block，或显式 metadata.continuation；
    - 只认 schema=oc-orch.continuation.v1（metadata 通道可省略 schema）；
    - 多个不同有效报告 → 保守 unknown（不猜）。
    """
    found: list[tuple[dict, str]] = []
    for msg in messages:
        if not isinstance(msg, dict) or msg.get("type") != "assistant":
            continue
        t = _message_created(msg)
        if t <= after_ms or t > until_ms:
            continue
        message_id = msg.get("id")
        meta = msg.get("metadata")
        if isinstance(meta, dict) and isinstance(meta.get("continuation"), dict):
            accepted = _accept_continuation(meta["continuation"],
                                            via_metadata=True)
            if accepted is not None:
                accepted["received_via"] = "metadata.continuation"
                found.append((accepted, str(message_id or "")))
        text = _message_text(msg).strip()
        candidates: list[dict] = []
        if text.startswith("{") and text.endswith("}"):
            try:
                candidates.append(json_loads(text, {}))
            except Exception:
                candidates = []
        for block in _FENCED_JSON.findall(text):
            try:
                candidates.append(json_loads(block, {}))
            except Exception:
                continue
        for candidate in candidates:
            accepted = _accept_continuation(candidate, via_metadata=False)
            if accepted is not None:
                accepted["received_via"] = "assistant-json"
                found.append((accepted, str(message_id or "")))
    if not found:
        return {"report": None, "reason": "no-structured-report"}
    first = found[0]
    for report, _ in found[1:]:
        if report != first[0]:
            return {"report": None, "reason": "multiple-continuation-reports"}
    report = dict(first[0])
    report["source_message_id"] = first[1]
    return {"report": report, "reason": None}


def _report_matches(row: dict, report: dict) -> bool:
    """报告绑定必须与责任/输入 tracking 精确一致。"""
    for key in _CONTINUATION_BINDING:
        actual, expected = report.get(key), row.get(key)
        if type(actual) is not type(expected) or actual != expected:
            return False
    return True


# S4 后续责任运行时常量（与 adapters.responsibility_ledger 的状态契约一致）。
_RESP_WAITING_EXECUTION = "waiting_execution"
_RESP_AWAITING_REVIEW = "awaiting_review"
_RESP_WAITING_HUMAN = "waiting_human"
_CONTINUATION_KIND = "continuation_ready"
_RESPONSIBILITY_MISSING_KIND = "responsibility_missing"
# 协调回合中断与项目暂停是不同语义：
# - coordinator-interrupted：只停止当前回合；下一次用户直接输入到达时自动恢复；
# - coordinator-paused：显式项目暂停；persistent manual gate，只有 unlock 才清除。
# 不要把“用户停止当前回答”升级成永久停调谐，否则直接续聊会表现为“输入一次动一下”。
_COORD_TURN_INTERRUPT_REASON = "coordinator-interrupted"
_COORD_PROJECT_PAUSE_REASON = "coordinator-paused"
# 缺失责任诊断至少经过两次正常 reconcile 才登记（第 1 次仅记疑似）。
_MISSING_RESPONSIBILITY_RECONCILES = 2
# 托管输入执行观察的短窗口：到期后由 SSE 触发或轮询做一次有界 REST 补证。
_EXECUTION_RECHECK_MS = 30 * 1000
_EXECUTION_SLOW_RECHECK_MS = 5 * 60 * 1000
# 有界公平扫描的页大小：持久游标 + id keyset；不用无限 limit 代替分页。
_SCAN_PAGE_LIMIT = 200
# 已托管未完成 Task 的状态（draft 尚未托管，不参与缺失责任诊断）。
_MANAGED_UNFINISHED_TASK_STATUSES = frozenset({
    "dispatched", "running", "blocked", "stalled", "result_pending",
    "result_received", "rejected",
})


def _message_created(m: dict) -> int:
    t = m.get("time") or {}
    try:
        return int(t.get("created") or 0)
    except (TypeError, ValueError):
        return 0


def _interval_coverage_label(coverage: dict | None) -> str:
    """把 read_message_window 的 coverage 映射为稳定标签。"""
    cov = coverage or {}
    if cov.get("error"):
        return "query-failed"
    if cov.get("complete") is False:
        return "insufficient"
    if cov.get("complete") is True:
        return "complete"
    return "unknown"


class SupervisorBusy(Exception):
    pass


def read_heartbeat(home: str) -> dict | None:
    path = os.path.join(home, HEARTBEAT_NAME)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def supervisor_status(home: str, cfg: dict | None = None) -> dict:
    cfg = cfg or load_config(home)
    hb = read_heartbeat(home) or {}
    pid = hb.get("pid")
    alive = False
    if isinstance(pid, int) and pid > 0:
        try:
            os.kill(pid, 0)
            alive = True
        except OSError:
            alive = False
    age = None
    if hb.get("at"):
        age = max(0, int((time.time() * 1000 - hb["at"]) / 1000))
    healthy = bool(alive and age is not None and age <= int(cfg["heartbeat_interval"]) * 3)
    return {
        "schema": "oc-orch.supervisor.v1",
        "home": home,
        "pid": pid,
        "alive": alive,
        "healthy": healthy,
        "heartbeat_age_s": age,
        "instance": hb.get("instance"),
        "started_at": hb.get("started_at"),
        "last_cycle_at": hb.get("last_cycle_at"),
        "connected": hb.get("connected"),
        "stream": hb.get("stream"),
        "counts": hb.get("counts"),
        "service": hb.get("service"),
        "last_error": hb.get("last_error"),
    }


class Supervisor:
    def __init__(self, home: str, *, cfg: dict | None = None, once: bool = False,
                 client: OpenCodeClient | None = None,
                 clock: Clock | None = None):
        self.home = ensure_home(home)
        self.cfg = cfg or load_config(home)
        self.once = once
        self.clock = clock if clock is not None else SystemClock()
        self.conn = init(self.home)
        # 同一可控时钟注入 Ledger：持久 deadline/通知期限与 supervisor 观察同源，
        # 避免 fake 只改变 supervisor 而台账期限仍走真实时间。
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        # S3：目标级额度冷却闸门（与 Supervisor 共用时钟，持久 meta、无网络）。
        self.target_gate = make_target_gate(self.conn, self.clock)
        self.coordinator_control = make_coordinator_control(self.conn, self.clock)
        self.turn_observation = make_turn_observation(self.conn, self.clock)
        self.client = client if client is not None else Client()
        self.instance = f"sup-{os.getpid()}-{short_id('', 6)}"
        self.started_at = self.clock.now_ms()
        self.stop_event = False
        self.last_cycle_at: int | None = None
        self.last_error: str | None = None
        self.service_failures = 0
        self.queue: "queue.Queue[dict]" = queue.Queue()
        self.stream = EventStream(self.client, self.cfg, self.queue)
        self.lock_fd: int | None = None
        self.owns_lock = False
        self._coord_cache: dict[str, dict | None] = {}
        self._coord_fail_handled: set[str] = set()
        # S4：受管输入/后续责任惰性装配（同一连接/时钟）；构造不访问网络。
        self._managed_inputs = None
        self._responsibilities_store = None
        self._dependency_wait = None
        self._operator_incident = None
        self._fault_recovery = None

    # ---- 惰性装配：提供商故障恢复（同一连接/时钟；构造不访问网络） ----
    def _fault_recovery_service(self):
        if self._fault_recovery is None:
            from ..composition import make_fault_recovery

            self._fault_recovery = make_fault_recovery(
                self.conn, self.home, client=lambda: self.client, clock=self.clock)
        return self._fault_recovery

    def observe_fault_failure(self, *, task: dict, att: dict,
                              terminal_error: dict | None) -> dict:
        """把一次已确认失败的执行回合登记为故障（provider 硬故障自动恢复队列）。"""
        if not terminal_error or not isinstance(terminal_error, dict):
            return {"action": "no-error"}
        try:
            return self._fault_recovery_service().observe_failure(
                error=terminal_error, task_id=task["id"], attempt_id=att["id"],
                session_id=att.get("session_id"),
                origin_at=att.get("terminal_at") or self.clock.now_ms())
        except Exception as exc:
            self.ledger.audit("supervisor", "fault.observe-error", "attempt",
                              att["id"], {"error": redact(repr(exc), 400)})
            return {"action": "error", "error": type(exc).__name__}

    def tick_fault_recovery(self, *, now: int) -> dict:
        """每轮有界推进故障恢复（无 episode 时零网络）。"""
        try:
            return self._fault_recovery_service().tick(limit=10, now=now)
        except Exception as exc:
            self.ledger.audit("supervisor", "fault.tick-error", "cycle",
                              str(self.last_cycle_at),
                              {"error": redact(repr(exc), 400)})
            return {"error": type(exc).__name__}

    # ---- 单实例 ---------------------------------------------------
    def acquire_lock(self) -> None:
        path = os.path.join(self.home, LOCK_NAME)
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            hb = read_heartbeat(self.home) or {}
            raise SupervisorBusy(
                f"已有 supervisor 持有写锁（pid={hb.get('pid')}）；"
                "单写者保证要求先停止旧实例"
            ) from None
        self.lock_fd = fd
        self.owns_lock = True
        atomic_write(
            os.path.join(self.home, PID_NAME),
            json.dumps({
                "pid": os.getpid(),
                "instance": self.instance,
                "started_at": self.started_at,
                "home": self.home,
            }, ensure_ascii=False, indent=1),
            0o600,
        )

    def release_lock(self) -> None:
        self.owns_lock = False
        if self.lock_fd is not None:
            try:
                fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(self.lock_fd)
            self.lock_fd = None

    # ---- 生命周期 -------------------------------------------------
    def run(self) -> int:
        self.acquire_lock()
        try:
            recovered = self.ledger.recover_inflight()
            self.ledger.set_meta("supervisor_instance", self.instance)
            self.ledger.set_meta("supervisor_started_at", str(self.started_at))
            if recovered:
                self.ledger.audit("supervisor", "startup.recover-inflight",
                                  "notification", "*", {"count": recovered})
            if not self.once:
                self.stream.start()
                # 给首个 server.connected 一点时间，但不等它——对账不依赖连接
                self.clock.sleep(0.2)
            self._safe_cycle(dispatch=True)
            if self.once:
                return 0
            interval = float(self.cfg["reconcile_interval"])
            next_cycle = self.clock.now_ms() / 1000.0 + interval
            while not self.stop_event:
                deadline = next_cycle - self.clock.now_ms() / 1000.0
                if deadline <= 0:
                    self._safe_cycle(dispatch=True)
                    next_cycle = self.clock.now_ms() / 1000.0 + interval
                    deadline = interval
                try:
                    item = self.queue.get(timeout=min(deadline, 0.5))
                except queue.Empty:
                    item = None
                if item is not None:
                    try:
                        urgent = self.handle_stream_item(item)
                    except Exception as e:
                        self.last_error = repr(e)
                        self.ledger.audit("supervisor", "event.error", "event",
                                          str(item.get("event", {}).get("id")),
                                          {"error": redact(repr(e), 1200)})
                        urgent = False
                    if urgent:
                        self._safe_cycle(dispatch=True)
                        next_cycle = self.clock.now_ms() / 1000.0 + interval
                if self.clock.now_ms() / 1000.0 >= next_cycle:
                    self._safe_cycle(dispatch=True)
                    next_cycle = self.clock.now_ms() / 1000.0 + interval
                self.write_heartbeat()
            return 0
        finally:
            try:
                self.stream.stop()
            except Exception:
                pass
            self.write_heartbeat(final=True)
            self.release_lock()

    # ---- 主循环 ---------------------------------------------------
    def _safe_cycle(self, *, dispatch: bool = True) -> dict:
        try:
            return self.cycle(dispatch=dispatch)
        except Exception as e:
            self.last_error = repr(e)
            self.ledger.audit("supervisor", "cycle.crash", "cycle",
                              str(self.clock.now_ms()), {"error": redact(repr(e), 2000)})
            return {"error": repr(e)}

    def cycle(self, *, dispatch: bool = True) -> dict:
        self.last_cycle_at = self.clock.now_ms()
        result: dict[str, Any] = {"at": self.last_cycle_at, "observed": 0, "dispatch": {}}
        service_ok = False
        try:
            info = self.client.info()
            service_ok = True
            base = self.client.service.base
            if self.ledger.get_meta("service_base") not in (None, base):
                self.ledger.audit("supervisor", "service.endpoint-changed", "service", base,
                                  {"old": self.ledger.get_meta("service_base")})
            self.ledger.set_meta("service_base", base)
            self.ledger.set_meta("service_version", str(info.get("version", "")))
            self.service_failures = 0
            self.last_error = None
        except Exception as e:
            self.service_failures += 1
            self.last_error = repr(e)
            self.ledger.set_meta("service_failures", str(self.service_failures))
            self.ledger.set_meta("last_service_error", redact(repr(e), 800))
            if self.service_failures >= 3:
                self.ledger.add_alert(
                    severity="high",
                    kind="service_unavailable",
                    title="OpenCode 服务不可用",
                    detail=(
                        f"连续 {self.service_failures} 次无法访问服务；"
                        f"待投递通知保留在台账中，服务恢复后会对账。错误：{redact(repr(e), 500)}"
                    ),
                    dedupe_key=f"service-down:{self.client.service.base}",
                )
        # S4：本地到期与责任期限先于 service 健康门。服务不可用时只跳过
        # 远端观察/补证，绝不跳过 park 到期、责任期限或缺失责任诊断。
        try:
            result["continuation"] = self.tick_continuation(allow_remote=service_ok)
        except Exception as e:
            self.last_error = repr(e)
            self.ledger.audit("supervisor", "continuation.error", "cycle",
                              str(self.last_cycle_at),
                              {"error": redact(repr(e), 1200)})
        self.publish_alerts()

        if not service_ok:
            self.write_heartbeat()
            self.publish_alerts()
            return result

        try:
            active = self.client.active_sessions()
            active_ok = True
        except Exception as e:
            # /active 只是优化；失败时仍要读 session 终态，不能让一次超时吞掉对账
            active = {}
            active_ok = False
            self.last_error = repr(e)
            self.ledger.audit("supervisor", "active.unavailable", "service",
                              self.client.service.base,
                              {"error": redact(repr(e), 500)})

        for att in self.ledger.open_attempts():
            try:
                self.observe_attempt(att, active, active_ok=active_ok)
                result["observed"] += 1
            except Exception as e:
                self.last_error = repr(e)
                self.ledger.audit("supervisor", "observe.error", "attempt", att["id"],
                                  {"error": redact(repr(e), 800)})

        try:
            self.finalize_terminals(active)
            self.observe_coordinators(active)
            self._fill_pending_routes()
            self.expire_notifications()
            # S2：未决输入跨重启持续对账（即使本轮 due 为空也执行）。
            self.reconcile_outstanding_inputs()
            self.ledger.set_next_checks(
                self.clock.now_ms() + int(self.cfg["reconcile_interval"]) * 1000
            )
        except Exception as e:
            self.last_error = repr(e)
            self.ledger.audit("supervisor", "cycle.error", "cycle", str(self.last_cycle_at),
                              {"error": redact(repr(e), 1200)})

        if dispatch:
            try:
                result["dispatch"] = self.dispatch_notifications()
            except Exception as e:
                self.last_error = repr(e)
                self.ledger.audit("supervisor", "dispatch.error", "cycle",
                                  str(self.last_cycle_at),
                                  {"error": redact(repr(e), 1200)})
        self.publish_alerts()
        self.write_heartbeat()
        return result

    # ---- S4：受管输入 / 后续责任运行时 -----------------------------
    def _responsibility_store(self):
        """惰性装配责任存储；构造不访问网络、不建表（schema v3 由 init 负责）。"""
        if self._responsibilities_store is None:
            self._responsibilities_store = make_responsibilities(self.ledger)
        return self._responsibilities_store

    def _dependency_wait_service(self):
        if self._dependency_wait is None:
            self._dependency_wait = make_dependency_wait(
                self.ledger, self.clock, store=self._responsibility_store())
        return self._dependency_wait

    def _managed_inputs_service(self):
        """惰性装配受管输入用例；与 Supervisor 共用连接、时钟与责任存储。"""
        if self._managed_inputs is None:
            self._managed_inputs = make_managed_inputs(
                self.conn, self.home, client=lambda: self.client,
                clock=self.clock,
                responsibilities=self._responsibility_store())
        return self._managed_inputs

    def observe_managed_inputs(self, *, allow_remote: bool = True) -> dict:
        """只观察显式白名单，默认空；到期只告警，不发送/flush/cancel/解闸门。

        ``allow_remote=False`` 时仍处理本地 park 到期：服务不可用不得跳过
        本地期限，也不得推进远端观察游标。
        """
        return self._managed_inputs_service().observe_registered(
            limit=20, allow_remote=allow_remote)

    def tick_continuation(self, *, allow_remote: bool) -> dict:
        """每轮先于 service 健康门的本地责任 tick。

        1. park/白名单本地到期（远端观察仅在服务健康时执行）；
        2. 责任 deadline/预算/接续发布（本地台账 + 原子通知入队）；
        3. 已托管未完成 Task 的有限缺失责任诊断（本地，至多两次 reconcile）。
        """
        now = self.clock.now_ms()
        out: dict[str, Any] = {
            "expiry": None, "responsibilities": 0, "published": 0,
            "waiting_human": 0, "deadline": 0, "ambiguous": 0,
            "missing": 0, "missing_suspected": 0,
        }
        try:
            observed = self.observe_managed_inputs(allow_remote=allow_remote)
            out["expiry"] = observed.get("expiry")
        except Exception as e:
            self.last_error = repr(e)
            self.ledger.audit("supervisor", "managed-input.observe-error", "cycle",
                              str(self.last_cycle_at),
                              {"error": redact(repr(e), 800)})
        try:
            out["dependencies"] = self._dependency_wait_service().reconcile(limit=50, now=now)
        except Exception as e:
            self.last_error = repr(e)
            self.ledger.audit("supervisor", "dependency.tick-error", "cycle",
                              str(self.last_cycle_at), {"error": redact(repr(e), 800)})
        try:
            out.update(self.tick_responsibilities(now=now, allow_remote=allow_remote))
        except Exception as e:
            self.last_error = repr(e)
            self.ledger.audit("supervisor", "responsibility.tick-error", "cycle",
                              str(self.last_cycle_at),
                              {"error": redact(repr(e), 800)})
        if allow_remote:
            try:
                out["inputs_reconciled"] = self.reconcile_registered_inputs(
                    now=now)
            except Exception as e:
                self.last_error = repr(e)
                self.ledger.audit("supervisor", "managed-input.reconcile-error",
                                  "cycle", str(self.last_cycle_at),
                                  {"error": redact(repr(e), 800)})
        try:
            missing = self.check_missing_responsibilities(now=now)
            out["missing"] = missing.get("created", 0)
            out["missing_suspected"] = missing.get("suspected", 0)
        except Exception as e:
            self.last_error = repr(e)
            self.ledger.audit("supervisor", "responsibility.missing-error", "cycle",
                              str(self.last_cycle_at),
                              {"error": redact(repr(e), 800)})
        try:
            out["business_health"] = self.continuity_health(now=now)
            self.ledger.set_meta("continuity_health", json_dumps(out["business_health"]))
        except Exception as e:
            self.last_error = repr(e)
            self.ledger.audit("supervisor", "continuity.health-error", "cycle",
                              str(self.last_cycle_at), {"error": redact(repr(e), 800)})
        try:
            if self._operator_incident is None:
                from ..composition import make_configured_operator_incident

                self._operator_incident = make_configured_operator_incident(
                    self.ledger, home=self.home, clock=self.clock)
            out["incidents"] = self._operator_incident.reconcile(limit=25)
        except Exception as e:
            self.last_error = repr(e)
            self.ledger.audit("supervisor", "incident.tick-error", "cycle",
                              str(self.last_cycle_at), {"error": redact(repr(e), 800)})
        try:
            out["fault_recovery"] = self.tick_fault_recovery(now=now)
        except Exception as e:
            self.last_error = repr(e)
            self.ledger.audit("supervisor", "fault.tick-error", "cycle",
                              str(self.last_cycle_at), {"error": redact(repr(e), 800)})
        return out

    def continuity_health(self, *, now: int | None = None) -> dict:
        """有界本地业务投影；控制状态与进程心跳分开，不为保活调用模型。"""
        from ..application.continuity_status import ContinuityStatusService

        return ContinuityStatusService(
            self._dependency_wait_service(), self.coordinator_control,
            self.target_gate, self.clock).snapshot(
                limit=50, now=now, advance_cursor=True)

    def tick_responsibilities(self, *, now: int, allow_remote: bool) -> dict:
        """处理到期責任：期限优先，其次接续发布/执行观察；预算跨重启不重置。"""
        counts = {"responsibilities": 0, "published": 0, "waiting_human": 0,
                  "deadline": 0, "ambiguous": 0, "recovered": 0, "pending": 0}
        store = self._responsibility_store()
        due = store.list_due_responsibilities(now=now, limit=50)
        seen = {row.get("id") for row in due}
        # 新建的托管发送责任只在“还没有任何 execution 观察标记”时做首次立即
        # 补证；首次观察后持续把 evidence.execution 写回去，后续严格按持久
        # next_check 到期（正常 running 不因 budget0 每轮绕开 next_check）。
        # SSE 触发仍经 _promote_responsibilities_for_session 例外提前。
        for row in self._scan_page_responsibilities():
            if row.get("id") in seen:
                continue
            exec_wait = (row.get("state") == _RESP_WAITING_EXECUTION or (
                row.get("state") in ("waiting_input", "handling")
                and row.get("wait_kind") in ("input_execution", "execution")))
            prior_exec = ((row.get("evidence") or {}).get("execution")
                          if isinstance(row.get("evidence"), dict) else None)
            if exec_wait and not prior_exec:
                due.append(row)
                seen.add(row.get("id"))
                continue
            try:
                task = self.ledger.get_task(row["task_id"])
            except LedgerError:
                continue
            if task.get("status") in ("accepted", "cancelled") \
                    or task.get("closed_at") is not None:
                due.append(row)
                seen.add(row.get("id"))
        for row in due:
            counts["responsibilities"] += 1
            try:
                outcome = self._tick_one_responsibility(
                    row, now=now, allow_remote=allow_remote)
            except LedgerError as e:
                # CAS/替代/预算等守卫拒绝：保留责任与诊断，不伪造完成。
                if e.code == "RESPONSIBILITY_STALE":
                    self._responsibility_waiting_human(
                        row, reason="stale-attribution", now=now,
                        detail=f"责任归属已变化：{str(e)[:300]}")
                elif e.code == "RESPONSIBILITY_TASK_TERMINAL":
                    self._closed_task_alert(row)
                else:
                    self.ledger.audit(
                        "supervisor", "responsibility.tick-rejected",
                        "responsibility", row.get("id"),
                        {"code": e.code, "message": str(e)[:400]})
                continue
            except Exception as e:
                self.ledger.audit("supervisor", "responsibility.tick-failed",
                                  "responsibility", row.get("id"),
                                  {"error": redact(repr(e), 500)})
                continue
            for key in ("published", "waiting_human", "deadline", "ambiguous",
                        "recovered", "pending"):
                if outcome.get(key):
                    counts[key] += 1
        return counts

    def _tick_one_responsibility(self, row: dict, *, now: int,
                                 allow_remote: bool) -> dict:
        # 依赖由独立的结构化条件对账负责；普通tick不得反复顺延旧字符串等待，
        # 也不得用另一去重键重复发布已登记的 dependency review。
        dependency = ((row.get("evidence") or {}).get("dependency")
                      if isinstance(row.get("evidence"), dict) else None)
        input_wait = row.get("wait_kind") in (
            "send_result_unknown", "cancel_reconcile", "input_execution", "execution")
        if (row.get("wait_kind") == "dependency"
                or row.get("state") == "waiting_dependency" and not input_wait):
            return {}
        if isinstance(dependency, dict) and dependency.get("satisfied") is True:
            return {}
        store = self._responsibility_store()
        task = None
        try:
            task = self.ledger.get_task(row["task_id"])
        except LedgerError:
            task = None
        status = (task or {}).get("status")
        if status in ("accepted", "cancelled"):
            # 仅真实验收/取消关闭对应业务推进责任；外停/写权清理另由既有守卫处理。
            reason = "task-cancelled" if status == "cancelled" else "task-accepted"
            store.close_responsibility(row["id"],
                                       expected_revision=row["revision"],
                                       reason=reason, actor="supervisor")
            self.ledger.audit("supervisor", "responsibility.closed-terminal",
                              "responsibility", row["id"],
                              {"task_status": status, "reason": reason})
            return {}
        if (task or {}).get("closed_at") is not None:
            # closed_at 但状态非 accepted/cancelled：人工可见，不冒充验收终结。
            self._closed_task_alert(row)
            return {}
        if status == "paused":
            # 暂停：只允许等待人工，不发布、不自动解除人工控制。
            if row.get("state") != _RESP_WAITING_HUMAN:
                store.update_responsibility(
                    row["id"], expected_revision=row["revision"],
                    fields={"state": _RESP_WAITING_HUMAN,
                            "wait_kind": "human",
                            "wait_condition": "task-paused",
                            "next_action": "任务暂停：人工决定后恢复；不自动发送",
                            "next_check_at": now + _EXECUTION_SLOW_RECHECK_MS},
                    actor="supervisor")
            return {"waiting_human": 1}

        state = row.get("state")
        if state == _RESP_WAITING_HUMAN:
            # 明确人工等待是持久 hold：due 检查不得自动离开、顺延或重复到期告警。
            return {}

        deadline = row.get("deadline")
        budget_used = int(row.get("budget_used") or 0)
        max_budget = int(row.get("max_budget") or 0)
        if (deadline is not None and now >= int(deadline)) \
                or budget_used >= max_budget:
            reason = ("deadline-exhausted" if deadline is not None
                      and now >= int(deadline) else "budget-exhausted")
            self._responsibility_waiting_human(
                row, reason=reason, now=now,
                detail=(f"责任期限/预算耗尽（deadline={deadline} "
                        f"budget={budget_used}/{max_budget}）；"
                        "业务仍未完成，转明确人工责任，不无限唤醒"))
            return {"waiting_human": 1,
                    "deadline": 1 if reason == "deadline-exhausted" else 0}

        if state == _RESP_AWAITING_REVIEW:
            result = store.publish_responsibility(
                row["id"], expected_revision=row["revision"],
                kind=_CONTINUATION_KIND,
                payload=self._continuation_payload(row, task),
                actor="supervisor")
            # 同事实下一检查退避：避免每轮 publish/audit；ACK 后期限仍保留。
            self._backoff_review(row, now=now)
            if result.get("published"):
                return {"published": 1}
            if result.get("reason") in ("no-route", "task-paused"):
                return {"waiting_human": 1}
            return {}

        wait_kind = row.get("wait_kind")
        if wait_kind in ("send_result_unknown", "cancel_reconcile"):
            if not allow_remote:
                return {}
            return self._reconcile_input_dependency(row, now=now)

        if state == _RESP_WAITING_EXECUTION or (
                state in ("waiting_input", "handling")
                and wait_kind in ("input_execution", "execution")):
            if not allow_remote:
                # 服务不可用：不消耗观察预算、不推进 next_check；本地期限已处理。
                return {}
            if not row.get("input_id") and not row.get("message_id"):
                self._responsibility_waiting_human(
                    row, reason="missing-input-identity", now=now,
                    detail="等待执行的责任缺少原输入/message 身份，无法定点补证")
                return {"waiting_human": 1}
            evidence = self._managed_execution_interval(row, now=now)
            return self._apply_execution_evidence(row, evidence, now=now)

        # 明确依赖等待：保留原因与责任人，有界推进下一次检查，不自动复活。
        store.update_responsibility(
            row["id"], expected_revision=row["revision"],
            fields={"next_check_at": now + _EXECUTION_SLOW_RECHECK_MS},
            actor="supervisor")
        return {}

    def _managed_execution_interval(self, row: dict, *, now: int) -> dict:
        """原输入定点 + 有界分页 history 建立真实执行区间。

        只用原 user message 的 created 作为下界、其后的 **idle 消息**作为执行
        结束锚点；旧 busy/idle、session.updated、session.outcome 一律不作为本
        输入已执行证据。区间内出现其他 user 输入 → ambiguous（不吞并别的输入
        执行）。覆盖不足/查询失败 → unknown，不登记结束事实。
        """
        session_id = row.get("session_id")
        message_id = row.get("message_id")
        if not session_id or not message_id:
            return {"state": "unknown", "reason": "no-input-identity"}
        anchor = self._point_input_message(session_id, message_id)
        if anchor is None:
            return {"state": "unknown", "reason": "anchor-unreadable"}
        input_at = _message_created(anchor)
        if input_at <= 0:
            return {"state": "unknown", "reason": "anchor-time-missing"}
        try:
            messages, coverage = self._window(
                session_id, anchor_id=message_id, since_ms=input_at,
                until_ms=now)
        except Exception as e:
            return {"state": "unknown", "reason": "query-failed",
                    "error": redact(repr(e), 200)}
        if not coverage.get("complete") or coverage.get("anchor_found") is not True:
            return {"state": "unknown", "reason": "coverage-incomplete",
                    "coverage": _interval_coverage_label(coverage)}
        if not any(m.get("id") == message_id and m.get("type") == "user"
                   for m in messages):
            return {"state": "unknown", "reason": "anchor-identity-mismatch"}
        ordered = sorted(messages, key=lambda m: (_message_created(m),
                                                  str(m.get("id") or "")))
        other_inputs: list[str] = []
        ended_at = None
        idle_id = None
        idle_outcome = None
        for msg in ordered:
            if msg.get("id") == message_id and msg.get("type") == "user":
                continue
            t = _message_created(msg)
            if t <= input_at:
                continue  # 锚点前旧回合：基线区域，不允许作为本输入证据
            if t > now:
                continue
            if ended_at is not None:
                break
            if msg.get("type") == "idle":
                ended_at = t
                idle_id = msg.get("id")
                idle_outcome = msg.get("outcome")
            elif msg.get("type") == "user":
                other_inputs.append(str(msg.get("id") or ""))
        if other_inputs:
            return {
                "state": "ambiguous",
                "reason": "other-input-in-interval",
                "input_at": input_at,
                "other_inputs": other_inputs[:5],
                "scanned": len(messages),
                "coverage": "complete",
                "explanation": ("同一区间出现其他 user 输入：无法证明 idle 只属于本输入，"
                                "保守 unknown/人工，不猜执行归属"),
            }
        if ended_at is None:
            return {"state": "running", "input_at": input_at,
                    "scanned": len(messages), "coverage": "complete"}
        parsed = extract_continuation_report(
            messages, after_ms=input_at, until_ms=ended_at)
        report = parsed.get("report")
        report_reason = parsed.get("reason")
        if report is not None and not _report_matches(row, report):
            report = None
            report_reason = "report-binding-mismatch"
        return {
            "state": "ended",
            "input_at": input_at,
            "ended_at": ended_at,
            "idle_id": idle_id,
            "idle_outcome": idle_outcome,
            "scanned": len(messages),
            "coverage": "complete",
            "report": report,
            "report_reason": report_reason,
            "report_source_message_id": (
                (report or {}).get("source_message_id") or None),
        }

    def _apply_execution_evidence(self, row: dict, evidence: dict, *,
                                  now: int) -> dict:
        store = self._responsibility_store()
        state = evidence.get("state")
        if state == "ended":
            # 结束事实先按原 ID 在输入回执登记 delivery + execution-only；
            # 回执未确认投递/执行时不发布接续待办（业务 ACK 保持独立）。
            fact = self._record_input_execution_fact(row, evidence)
            if not fact.get("recorded"):
                self.ledger.audit(
                    "supervisor", "responsibility.execution-unreceipted",
                    "responsibility", row["id"],
                    {"reason": fact.get("reason"),
                     "input_id": row.get("input_id")})
                merged = dict(row.get("evidence") or {})
                merged["execution"] = {
                    **evidence,
                    "receipt": {"recorded": False,
                                "reason": fact.get("reason")},
                }
                store.update_responsibility(
                    row["id"], expected_revision=row["revision"],
                    fields={"wait_condition": "execution-receipt-pending",
                            "next_action": ("执行事实尚不能按原输入登记："
                                            "保持等待并定点补证，不发布接续"),
                            "next_check_at": now + _EXECUTION_RECHECK_MS,
                            "evidence": merged},
                    actor="supervisor")
                return {}
            merged = dict(row.get("evidence") or {})
            merged["execution"] = {
                "input_at": evidence.get("input_at"),
                "ended_at": evidence.get("ended_at"),
                "idle_id": evidence.get("idle_id"),
                "idle_outcome": evidence.get("idle_outcome"),
                "scanned": evidence.get("scanned"),
                "coverage": evidence.get("coverage"),
                "receipt": {"recorded": True,
                            "changed": fact.get("changed"),
                            "delivery": fact.get("delivery")},
            }
            report = evidence.get("report")
            report_error = evidence.get("report_reason")
            if isinstance(report, dict):
                if self._record_business_report(row, report, evidence):
                    merged["continuation"] = self._continuation_evidence(
                        report, now=now)
                else:
                    report = None
                    report_error = "business-report-persist-rejected"
            else:
                report = None
            if report_error:
                merged["continuation_error"] = report_error
            kind = (report or {}).get("kind")
            classification = CONTINUATION_CLASSIFICATION.get(
                kind, "AWAITING_REVIEW")
            next_action = ((report or {}).get("next_action")
                           or ("执行已结束但无结构化业务回报：协调者核对交付物/"
                               "补材料/决定是否新 attempt 返工；不得自动 accepted"))
            wait_condition = (f"business-report:{kind}" if kind
                              else "business-report-unknown")
            try:
                updated = store.update_responsibility(
                    row["id"], expected_revision=row["revision"],
                    fields={
                        "state": _RESP_AWAITING_REVIEW,
                        "wait_kind": "review",
                        "wait_condition": wait_condition,
                        "next_action": next_action,
                        "next_check_at": now,
                        "evidence": merged,
                    },
                    actor="supervisor")
                result = store.publish_responsibility(
                    updated["id"], expected_revision=updated["revision"],
                    kind=_CONTINUATION_KIND,
                    payload=self._continuation_payload(
                        updated, None, classification=classification),
                    actor="supervisor")
            except LedgerError as e:
                if e.code == "RESPONSIBILITY_STALE":
                    self._responsibility_waiting_human(
                        row, reason="stale-attribution", now=now,
                        detail=f"执行结束事实归属已变化：{str(e)[:300]}")
                    return {"waiting_human": 1}
                if e.code == "RESPONSIBILITY_TASK_TERMINAL":
                    self._closed_task_alert(row)
                    return {}
                if e.code == "RESPONSIBILITY_NOT_REVIEWABLE":
                    self._responsibility_waiting_human(
                        row, reason="review-fact-missing", now=now,
                        detail=f"发布门拒绝：{str(e)[:300]}")
                    return {"waiting_human": 1}
                raise
            self._backoff_review(updated, now=now)
            if result.get("published"):
                return {"published": 1}
            return {"waiting_human": 1} if result.get("reason") == "no-route" else {}
        if state == "ambiguous":
            merged = dict(row.get("evidence") or {})
            merged["execution"] = evidence
            try:
                store.update_responsibility(
                    row["id"], expected_revision=row["revision"],
                    fields={"budget_used": int(row.get("budget_used") or 0) + 1,
                            "wait_condition": "execution-interval-ambiguous",
                            "next_action": ("执行区间混入其他输入，无法归属本输入："
                                            "保留证据待人工/定点补证，不猜结束"),
                            "next_check_at": now + _EXECUTION_SLOW_RECHECK_MS,
                            "evidence": merged},
                    actor="supervisor")
            except LedgerError as e:
                if e.code == "RESPONSIBILITY_BUDGET_EXHAUSTED":
                    self._responsibility_waiting_human(
                        row, reason="execution-evidence-ambiguous", now=now,
                        detail="执行区间一直无法唯一归属，未知/失败补证预算已耗尽")
                    return {"waiting_human": 1}
                raise
            return {"ambiguous": 1}
        if state == "unknown":
            # 原输入仍无可信锚点（很可能仍 pending）：先走有界原 ID 恢复路径，
            # 不把它当成未知/失败补证烧预算（pending 宽限不提前耗尽）。
            if evidence.get("reason") == "anchor-unreadable":
                handled = self._pending_execution_path(row, evidence, now=now)
                if handled is not None:
                    return handled
            # 查询失败/覆盖不足：只烧未知/失败重试预算，耗尽转人工。
            merged = dict(row.get("evidence") or {})
            merged["execution"] = evidence
            try:
                store.update_responsibility(
                    row["id"], expected_revision=row["revision"],
                    fields={"budget_used": int(row.get("budget_used") or 0) + 1,
                            "next_check_at": now + _EXECUTION_RECHECK_MS,
                            "evidence": merged},
                    actor="supervisor")
            except LedgerError as e:
                if e.code == "RESPONSIBILITY_BUDGET_EXHAUSTED":
                    self._responsibility_waiting_human(
                        row, reason="execution-observation-exhausted", now=now,
                        detail=f"执行观察补证预算耗尽：{evidence.get('reason')}")
                    return {"waiting_human": 1}
                raise
            return {}
        # running：正常执行等待只按 deadline 约束，不消耗错误/未知补证预算。
        merged = dict(row.get("evidence") or {})
        merged["execution"] = evidence
        store.update_responsibility(
            row["id"], expected_revision=row["revision"],
            fields={"next_check_at": now + _EXECUTION_RECHECK_MS,
                    "evidence": merged},
            actor="supervisor")
        return {}

    def _pending_execution_path(self, row: dict, evidence: dict, *, now: int) -> dict | None:
        """原输入疑似仍 pending（锚点不可读）的有界恢复路径。

        - 未装配 B.recover_pending_execute → 返回 None，沿用原 unknown 补证；
        - 宽限期内 / 上一次尝试仍在节流窗口 → 只推进 next_check，不烧失败预算
          （正常 pending 等待不占失败额度）；
        - 超过宽限 → 调用共享用例 recover_pending_execute(input_id, grace_ms=...)；
          recovered 只代表“已按原 ID steer/scheduled”，绝不等于 executed，之后
          仍由 REST 原 ID 观测；blocked/foreign/gate/control/unknown 保留可见责任
          与原因，明确不新建 prompt、不自动恢复。
        """
        managed = self._managed_inputs_service()
        recover = getattr(managed, "recover_pending_execute", None)
        if not callable(recover):
            return None
        store = self._responsibility_store()
        rec = dict(row.get("evidence") or {})
        prev = rec.get("recovery") if isinstance(rec.get("recovery"), dict) else {}
        last = prev.get("attempted_at")
        if isinstance(last, int) and not isinstance(last, bool) \
                and now - last < _EXECUTION_SLOW_RECHECK_MS:
            merged = dict(rec); merged["execution"] = evidence
            store.update_responsibility(
                row["id"], expected_revision=row["revision"],
                fields={"next_check_at": now + _EXECUTION_SLOW_RECHECK_MS,
                        "evidence": merged}, actor="supervisor")
            return {"pending": 1}
        created = int(row.get("created_at") or now)
        grace_ms = int(self.cfg.get("dispatch_unknown_grace") or 120) * 1000
        if now - created < grace_ms:
            merged = dict(rec); merged["execution"] = evidence
            store.update_responsibility(
                row["id"], expected_revision=row["revision"],
                fields={"next_check_at": now + _EXECUTION_RECHECK_MS,
                        "evidence": merged}, actor="supervisor")
            return {"pending": 1}
        try:
            result = recover(row.get("input_id"), grace_ms=grace_ms)
        except Exception as e:
            merged = dict(rec); merged["execution"] = evidence
            merged["recovery"] = {"attempted_at": now, "state": "error",
                                  "reason": redact(repr(e), 200)}
            store.update_responsibility(
                row["id"], expected_revision=row["revision"],
                fields={"wait_condition": "pending-recover-error",
                        "next_action": "恢复调用失败：保留责任原因，按期限有界等待",
                        "next_check_at": now + _EXECUTION_SLOW_RECHECK_MS,
                        "evidence": merged}, actor="supervisor")
            return {"pending": 1}
        result = result if isinstance(result, dict) else {}
        recovered = bool(result.get("recovered") or result.get("patched"))
        blocked = bool(result.get("blocked"))
        reason = str(result.get("reason") or result.get("state") or "unknown")
        rec["execution"] = evidence
        rec["recovery"] = {"attempted_at": now, "recovered": recovered,
                           "blocked": blocked, "reason": reason[:200],
                           "grace_ms": grace_ms}
        fields: dict[str, Any] = {"evidence": rec}
        if recovered:
            fields.update({
                "wait_condition": "pending-recovered",
                "next_action": ("原 ID 已恢复（steer/scheduled）：继续 REST 原 ID "
                                "观测，不把 steer 当 executed"),
                "next_check_at": now + _EXECUTION_RECHECK_MS,
            })
        else:
            fields.update({
                "wait_condition": "pending-recover-blocked",
                "next_action": (f"保留责任原因（{reason[:200]}）：明确不自动恢复/"
                                "不新建 prompt，超期限转人工"),
                "next_check_at": now + _EXECUTION_SLOW_RECHECK_MS,
            })
        store.update_responsibility(
            row["id"], expected_revision=row["revision"],
            fields=fields, actor="supervisor")
        return {"recovered": 1 if recovered else 0, "pending": 1}

    def _record_input_execution_fact(self, row: dict, evidence: dict) -> dict:
        """先按原 ID 登记 delivered，再写 execution-only；不触碰 business_ack。"""
        input_id = row.get("input_id")
        if not input_id:
            return {"recorded": False, "reason": "no-input-identity"}
        outcome = evidence.get("idle_outcome")
        if outcome not in ("succeeded", "failed", "interrupted"):
            return {"recorded": False, "reason": "outcome-unknown"}
        managed = self._managed_inputs_service()
        try:
            receipt = managed.reconcile(input_id).get("input") or {}
        except Exception as e:
            return {"recorded": False,
                    "reason": f"delivery-reconcile-failed:{redact(repr(e), 120)}"}
        if receipt.get("delivery") != "delivered":
            return {"recorded": False,
                    "reason": f"delivery-{receipt.get('delivery')}",
                    "delivery": receipt.get("delivery")}
        execution = {
            "outcome": outcome,
            "message_id": row.get("message_id"),
        }
        for key in ("attempt_id", "generation", "result_version"):
            value = receipt.get(key)
            if value is not None:
                execution[key] = value
        try:
            out = managed.record_managed_observation(
                input_id, execution=execution,
                via="runtime:execution-interval",
                evidence={
                    "source": "runtime:execution-interval",
                    "detail": {
                        "input_at": evidence.get("input_at"),
                        "ended_at": evidence.get("ended_at"),
                        "idle_id": evidence.get("idle_id"),
                        "scanned": evidence.get("scanned"),
                        "coverage": evidence.get("coverage"),
                    },
                })
        except Exception as e:
            return {"recorded": False,
                    "reason": f"record-failed:{redact(repr(e), 160)}"}
        return {"recorded": bool(out.get("recorded", True)),
                "changed": bool(out.get("changed")),
                "delivery": "delivered",
                "reason": out.get("reason")}

    def _reconcile_input_dependency(self, row: dict, *, now: int) -> dict:
        """send_result_unknown/cancel_reconcile：只按原 ID 补证，不换身份重投。"""
        input_id = row.get("input_id")
        if not input_id:
            return {}
        managed = self._managed_inputs_service()
        try:
            receipt = managed.reconcile(input_id).get("input") or {}
        except Exception as e:
            merged = dict(row.get("evidence") or {})
            merged["dependency_reconcile"] = {
                "at": now, "state": "unknown",
                "reason": redact(repr(e), 160)}
            self._responsibility_store().update_responsibility(
                row["id"], expected_revision=row["revision"],
                fields={"next_check_at": now + _EXECUTION_SLOW_RECHECK_MS,
                        "evidence": merged}, actor="supervisor")
            return {}
        delivery = receipt.get("delivery")
        merged = dict(row.get("evidence") or {})
        merged["dependency_reconcile"] = {
            "at": now, "delivery": delivery,
            "admission": receipt.get("admission")}
        fields: dict[str, Any] = {"evidence": merged}
        if delivery == "cancelled":
            fields.update({
                "state": _RESP_WAITING_HUMAN, "wait_kind": "human",
                "wait_condition": "input-cancelled",
                "next_action": ("输入已由可信取消证据确证：保留审计，由责任人"
                                "决定后续；不自动重投、不自动关闭业务责任"),
                "next_check_at": now + _EXECUTION_SLOW_RECHECK_MS,
            })
            self._responsibility_store().update_responsibility(
                row["id"], expected_revision=row["revision"],
                fields=fields, actor="supervisor")
            return {"waiting_human": 1}
        if delivery == "delivered":
            fields.update({
                "state": _RESP_WAITING_EXECUTION, "wait_kind": "input_execution",
                "wait_condition": f"input:{row.get('message_id')}:execution",
                "next_action": "已确认投递：等待本输入归属执行结束并核收",
                "next_check_at": now,
            })
            self._responsibility_store().update_responsibility(
                row["id"], expected_revision=row["revision"],
                fields=fields, actor="supervisor")
            return {}
        # 仍 unknown/pending：只按原 ID 有界补证，消耗未知重试预算。
        fields.update({
            "budget_used": int(row.get("budget_used") or 0) + 1,
            "wait_condition": "send-result-unknown",
            "next_check_at": now + _EXECUTION_RECHECK_MS,
        })
        try:
            self._responsibility_store().update_responsibility(
                row["id"], expected_revision=row["revision"],
                fields=fields, actor="supervisor")
        except LedgerError as e:
            if e.code == "RESPONSIBILITY_BUDGET_EXHAUSTED":
                self._responsibility_waiting_human(
                    row, reason="send-result-unknown", now=now,
                    detail="发送结果长期未知，原 ID 补证预算耗尽")
                return {"waiting_human": 1}
            raise
        return {}


    def _continuation_evidence(self, report: dict, *, now: int) -> dict:
        out = {
            "schema": CONTINUATION_SCHEMA,
            "kind": report.get("kind"),
            "next_action": report.get("next_action"),
            "source_message_id": report.get("source_message_id"),
            "received_at": now,
            "via": report.get("received_via"),
            "input_id": report.get("input_id"),
            "task_id": report.get("task_id"),
            "attempt_id": report.get("attempt_id"),
            "generation": report.get("generation"),
            "result_version": report.get("result_version"),
        }
        for key in _CONTINUATION_PAYLOAD_KEYS:
            if key in report:
                out[key] = report.get(key)
        return out

    def _record_business_report(self, row: dict, report: dict,
                                evidence: dict) -> bool:
        """B 的持久报告入口（若已装配）；拒绝/异常按非法报告处理（unknown）。"""
        input_id = row.get("input_id")
        if not input_id:
            return False
        managed = self._managed_inputs_service()
        record = getattr(managed, "record_business_report", None)
        if not callable(record):
            # B 分阶段交付：C 仍把已校验报告持久写入责任 evidence.continuation。
            return True
        source_msg_id = report.get("source_message_id")
        if not isinstance(source_msg_id, str) or not source_msg_id.strip():
            return False  # 无真实 assistant 来源消息：不传给 B（不混淆 user input id）
        try:
            record(input_id, report=dict(report),
                   source_message_id=source_msg_id,
                   via="runtime:continuation-report")
        except Exception as e:
            self.ledger.audit(
                "supervisor", "responsibility.report-rejected",
                "responsibility", row.get("id"),
                {"input_id": input_id, "error": redact(repr(e), 400),
                 "source_message_id": report.get("source_message_id")})
            return False
        return True

    def _backoff_review(self, row: dict, *, now: int) -> None:
        """同一 awaiting_review 事实的下一检查退避；不缩短/重置 deadline。"""
        try:
            self._responsibility_store().update_responsibility(
                row["id"], expected_revision=row["revision"],
                fields={"next_check_at": now + _EXECUTION_SLOW_RECHECK_MS},
                actor="supervisor")
        except LedgerError:
            # 发布内部已转 waiting_human 或并发 revision：保留其状态。
            pass

    def _closed_task_alert(self, row: dict) -> None:
        self.ledger.add_alert(
            severity="warning", kind="task_closed_nonterminal",
            title=f"任务非验收关闭仍阻责任：{row.get('task_id')}",
            detail=(f"task={row.get('task_id')} 已 closed_at 但状态非 "
                    "accepted/cancelled；不冒充验收终结，责任保留待人工确认"),
            dedupe_key=f"task-closed-nonterminal:{row.get('task_id')}",
            task_id=row.get("task_id"), attempt_id=row.get("attempt_id"))

    def _continuation_payload(self, row: dict, task: dict | None, *,
                              classification: str | None = None) -> dict:
        evidence = row.get("evidence") or {}
        execution = evidence.get("execution") or {}
        continuation = evidence.get("continuation") if isinstance(
            evidence.get("continuation"), dict) else {}
        kind = continuation.get("kind")
        classification = (classification
                          or CONTINUATION_CLASSIFICATION.get(
                              kind, "AWAITING_REVIEW"))
        payload = {
            "classification": classification,
            "summary": (f"托管输入执行结束：结构化报告 kind={kind}"
                        if kind else
                        "托管输入执行结束，缺少可确认的结构化业务回报："
                        "由协调者判断完成/补料/返工"),
            "input_id": row.get("input_id"),
            "message_id": row.get("message_id"),
            "session_id": row.get("session_id"),
            "execution": execution,
            "materials": evidence.get("materials") or [],
            "next_action": row.get("next_action"),
            "auto_accept": False,
        }
        if continuation:
            payload["continuation"] = continuation
            for key in ("kind", "source_message_id", "materials",
                        "missing_materials", "decision", "evidence_refs"):
                if continuation.get(key) is not None:
                    payload[key] = continuation.get(key)
        return payload

    def _responsibility_waiting_human(self, row: dict, *, reason: str,
                                      now: int, detail: str) -> None:
        store = self._responsibility_store()
        merged = dict(row.get("evidence") or {})
        merged["blocked"] = {"reason": reason, "at": now, "detail": detail[:400]}
        try:
            store.update_responsibility(
                row["id"], expected_revision=row["revision"],
                fields={"state": _RESP_WAITING_HUMAN,
                        "wait_kind": "human",
                        "wait_condition": reason,
                        "next_action": f"人工处理（{reason}）：{detail[:200]}",
                        "next_check_at": now + _EXECUTION_SLOW_RECHECK_MS,
                        "evidence": merged},
                actor="supervisor")
        except LedgerError as e:
            self.ledger.audit("supervisor", "responsibility.human-rejected",
                              "responsibility", row.get("id"),
                              {"code": e.code, "message": str(e)[:300]})
        self.ledger.add_alert(
            severity="high", kind="responsibility_blocked",
            title=f"接续责任转人工：{row.get('task_id')}",
            detail=(f"责任 {row.get('id')} task={row.get('task_id')} "
                    f"owner={row.get('owner')} 原因={reason}；{detail}"),
            dedupe_key=(f"resp-human:{row.get('id')}:{reason}:"
                        f"{row.get('condition_key')}:{row.get('generation')}:"
                        f"{row.get('result_version')}"),
            task_id=row.get("task_id"), attempt_id=row.get("attempt_id"),
        )

    def _promote_responsibilities_for_session(self, session_id: str | None) -> int:
        """SSE 触发：把该 session 的等待执行责任提前到本轮做一次 REST 补证。"""
        if not session_id:
            return 0
        store = self._responsibility_store()
        promoted = 0
        now = self.clock.now_ms()
        for row in self._scan_page_responsibilities(session_id=session_id):
            dependency = row.get("wait_kind") in ("send_result_unknown",
                                                 "cancel_reconcile")
            if not (row.get("state") == _RESP_WAITING_EXECUTION or dependency or (
                    row.get("state") in ("waiting_input", "handling")
                    and row.get("wait_kind") in ("input_execution", "execution"))):
                continue
            if row.get("session_id") != session_id:
                continue
            if int(row.get("next_check_at") or 0) <= now:
                promoted += 1
                continue
            try:
                store.update_responsibility(
                    row["id"], expected_revision=row["revision"],
                    fields={"next_check_at": now}, actor="sse")
                promoted += 1
            except LedgerError:
                continue
        return promoted

    def check_missing_responsibilities(self, *, now: int) -> dict:
        """已托管未完成 Task 的有限缺失责任检查。

        - 任务必须有真实 attempt 且处于托管未完成状态；draft 不参与。
        - 有有效 open attempt 等待记录或已有 open 责任 → 清空疑似计数，不催办。
        - 缺口连续出现：第 1 次只记疑似，第 2 次登记稳定 responsibility_missing；
          条件唯一索引保证每轮不新建。
        - 缺失责任通知走 publish（原子）+ 既有 dispatch 单槽/gate/冷却守卫。
        """
        store = self._responsibility_store()
        out = {"created": 0, "suspected": 0, "scanned": 0}
        for task in self._scan_page_tasks():
            out["scanned"] += 1
            status = task.get("status")
            if status not in _MANAGED_UNFINISHED_TASK_STATUSES:
                continue
            if task.get("closed_at") is not None:
                continue
            attempts = self.ledger.list_attempts(task_id=task["id"], limit=50)
            if not attempts:
                continue  # 尚无托管尝试：Task 尚未进入托管
            if self._task_has_open_attempt(task):
                self._clear_missing_marker(task["id"])
                continue
            if self._task_has_delivery_pending(task):
                # 待办仍在投递（pending/inflight/sent 未 ACK）：不另发 missing。
                self._clear_missing_marker(task["id"])
                continue
            if store.list_open_responsibilities(task_id=task["id"], limit=1):
                self._clear_missing_marker(task["id"])
                continue
            result = self._note_missing_responsibility(task, now=now)
            if result == "created":
                out["created"] += 1
            elif result == "suspected":
                out["suspected"] += 1
        return out

    def _page_unfinished_tasks(self, *, after_id=None, limit=200) -> list[dict]:
        """A keyset API 优先；未就绪时用有界快照按 id 升序模拟同一契约。"""
        store = self._responsibility_store()
        fn = getattr(store, "list_unfinished_tasks_page", None)
        if callable(fn):
            return list(fn(after_id=after_id, limit=limit))
        rows = [t for t in self.ledger.list_tasks(limit=5000)
                if t.get("status") not in ("accepted", "cancelled")
                and t.get("closed_at") is None]
        if len(rows) >= 5000:
            self._note_scan_overflow("tasks", 5000)
        rows.sort(key=lambda t: str(t.get("id") or ""))
        if after_id:
            rows = [t for t in rows if str(t.get("id") or "") > after_id]
        return rows[:limit]

    def _page_open_responsibilities(self, *, after_id=None, session_id=None,
                                    limit=200) -> list[dict]:
        """A keyset API 优先；过渡实现按 Task 分页收集后按 id 排序截断。"""
        store = self._responsibility_store()
        fn = getattr(store, "list_open_responsibilities_page", None)
        if callable(fn):
            return list(fn(after_id=after_id, session_id=session_id,
                           limit=limit))
        collected: list[dict] = []
        seen: set[str] = set()
        for task in self.ledger.list_tasks(limit=5000):
            for row in store.list_open_responsibilities(
                    task_id=task["id"], limit=501):
                rid = row.get("id")
                if rid in seen:
                    continue
                if session_id and row.get("session_id") != session_id:
                    continue
                if after_id and str(rid or "") <= after_id:
                    continue
                seen.add(rid)
                collected.append(row)
        collected.sort(key=lambda r: str(r.get("id") or ""))
        return collected[:limit]

    def _scan_page_tasks(self) -> list[dict]:
        """按持久游标取有界多页未完成 Task；短页后回绕到头部。"""
        return self._scan_pages(
            lambda after: self._page_unfinished_tasks(
                after_id=after, limit=_SCAN_PAGE_LIMIT + 1),
            "sweep-cursor:tasks")

    def _scan_page_responsibilities(self, *, session_id=None) -> list[dict]:
        """按持久游标取有界多页未关闭责任；短页后回绕，不写 SQL、不抬 limit。"""
        key = f"sweep-cursor:responsibilities:{session_id or '*'}"
        return self._scan_pages(
            lambda after: self._page_open_responsibilities(
                after_id=after, session_id=session_id,
                limit=_SCAN_PAGE_LIMIT + 1),
            key)

    def _scan_pages(self, fetch, key: str, *,
                    max_pages: int = 3) -> list[dict]:
        """keyset 页 + 持久游标：每轮有界页数，尾部回绕，避免头部饿死尾部。"""
        after = self.ledger.get_meta(key) or None
        out: list[dict] = []
        seen: set[str] = set()
        for _ in range(max_pages):
            rows = fetch(after)
            if not rows and after:
                after = None
                rows = fetch(None)
            page = rows[:_SCAN_PAGE_LIMIT]
            for row in page:
                rid = str(row.get("id") or "")
                if rid in seen:
                    continue
                seen.add(rid)
                out.append(row)
            if not page:
                after = None
                break
            has_more = len(rows) > _SCAN_PAGE_LIMIT
            after = page[-1].get("id") if has_more else None
            if not has_more:
                break
        self.ledger.set_meta(key, after or "")
        return out

    def reconcile_registered_inputs(self, *, now: int) -> int:
        """outstanding=0 的 managed/park 输入：B 的 keyset 页 + 持久游标公平对账。

        B(``reconcile_registered_inputs_page``) 每页按原 ID reconcile（只读），
        不 flush/cancel、不创建 Task/责任、不猜 execution；C 只维护持久
        ``next_after_id``，短页/空页后回绕。未装配页 API 时明确审计，不以前
        500 头快照冒充全集（不写外层 SQL、不抬无限 limit）。
        """
        managed = self._managed_inputs_service()
        page = getattr(managed, "reconcile_registered_inputs_page", None)
        if not callable(page):
            self.ledger.audit(
                "supervisor", "managed-input.reconcile-page-missing",
                "cycle", str(self.last_cycle_at),
                {"error": "reconcile_registered_inputs_page 未装配，跳过本轮补证"})
            return 0
        key = "sweep-cursor:managed-inputs"
        after = self.ledger.get_meta(key) or None
        total = 0
        for _ in range(3):  # 每轮有界页数，短页回绕
            result = page(after_id=after, limit=_SCAN_PAGE_LIMIT)
            count = int(result.get("count") or 0)
            total += count
            after = result.get("next_after_id") or None
            if not count or not result.get("has_more"):
                break
        self.ledger.set_meta(key, after or "")
        return total

    def _note_scan_overflow(self, what: str, limit: int,
                            task_id: str | None = None) -> None:
        """有界扫描超限：可见诊断，不把截断当“没有更多”。"""
        self.ledger.add_alert(
            severity="warning", kind="bounded_scan_overflow",
            title=f"有界扫描达到上限：{what}",
            detail=(f"{what} 扫描上限 {limit} 已用尽且仍有更多记录；"
                    "本轮只处理已扫描部分，后续轮次继续；需评估分页/游标实现"),
            dedupe_key=f"scan-overflow:{what}:{task_id or '*'}",
            task_id=task_id)

    def _task_has_delivery_pending(self, task: dict) -> bool:
        """待办仍 pending/inflight/sent 未 ACK（适用当前归属）：投递中不发 missing。"""
        fn = getattr(self._responsibility_store(),
                     "has_pending_task_notifications", None)
        if not callable(fn):
            self.ledger.audit(
                "supervisor", "responsibility.missing-pending-check-missing",
                "task", task.get("id"),
                {"error": "has_pending_task_notifications 未装配，保守跳过缺失责任检查"})
            return True  # 未装配时保守：不新建 missing（按“有投递待办”跳过）
        return fn(task["id"]) > 0
    def _task_has_open_attempt(self, task: dict) -> bool:
        if task.get("current_attempt_id"):
            try:
                current = self.ledger.get_attempt(task["current_attempt_id"])
            except LedgerError:
                current = None
            if current and current.get("status") not in ATTEMPT_TERMINAL \
                    and current.get("site_released_at") is None:
                return True
        for att in self.ledger.open_attempts(task_id=task["id"]):
            if att.get("site_released_at") is None:
                return True
        return False

    def _missing_marker_key(self, task_id: str) -> str:
        return f"missing-responsibility:{task_id}"

    def _clear_missing_marker(self, task_id: str) -> None:
        if self.ledger.get_meta(self._missing_marker_key(task_id)) is not None:
            self.ledger.set_meta(self._missing_marker_key(task_id), "{}")

    def _note_missing_responsibility(self, task: dict, *, now: int) -> str:
        key = self._missing_marker_key(task["id"])
        raw = self.ledger.get_meta(key)
        try:
            rec = json_loads(raw, {}) if isinstance(raw, str) else {}
        except Exception:
            rec = {}
        rec = rec if isinstance(rec, dict) else {}
        count = int(rec.get("count") or 0) + 1
        if count < _MISSING_RESPONSIBILITY_RECONCILES:
            self.ledger.set_meta(key, json_dumps({
                "count": count, "first_at": rec.get("first_at") or now,
                "last_at": now, "reason": "no-open-responsibility",
            }))
            return "suspected"
        self._open_missing_responsibility(task, now=now, count=count)
        return "created"

    def _task_route(self, task: dict) -> tuple[str | None, str]:
        """当前可核验的接续目标：任务 coordinator 优先，其次项目租约 holder。"""
        coord = task.get("coordinator_session")
        if isinstance(coord, str) and coord:
            return coord, "task-coordinator"
        try:
            lease = self.ledger.coordinator_lease(task.get("project") or "")
        except Exception:
            lease = None
        holder = (lease or {}).get("holder_session") if isinstance(lease, dict) else None
        if isinstance(holder, str) and holder:
            return holder, "lease-holder"
        return None, "no-route"

    def _open_missing_responsibility(self, task: dict, *, now: int,
                                     count: int) -> dict:
        store = self._responsibility_store()
        target, route = self._task_route(task)
        owner = target or "operator"
        state = _RESP_AWAITING_REVIEW if target else _RESP_WAITING_HUMAN
        current_attempt_id = task.get("current_attempt_id")
        attempt = None
        if current_attempt_id:
            try:
                attempt = self.ledger.get_attempt(current_attempt_id)
            except LedgerError:
                attempt = None
        generation = attempt.get("generation") if attempt else None
        result_version = (((attempt or {}).get("metadata") or {})
                          .get("result") or {}).get("version")
        opened = store.open_responsibility(
            owner=owner, project=task.get("project") or "",
            task_id=task["id"], attempt_id=current_attempt_id,
            generation=generation, result_version=result_version,
            target_session=target, state=state,
            wait_kind="review" if target else "human",
            wait_condition="missing-responsibility",
            next_action=(
                "已托管未完成 Task 且无有效推进/等待责任：协调者补齐责任或明确"
                "人工处理；不得把空通知队列/租约到期当作已处理"
                if target else
                "已托管未完成 Task 且无可核验协调者：转人工指定责任人"),
            next_check_at=now, deadline=now + 24 * 3600 * 1000,
            condition_key="missing-responsibility",
            evidence={
                "detected_at": now, "reconciles": count, "route": route,
                "task_status": task.get("status"),
                "current_attempt_id": current_attempt_id,
                "reason": "no-open-responsibility",
            },
            max_budget=2, actor="supervisor")
        row = (opened.get("responsibility")
               if isinstance(opened, dict)
               and isinstance(opened.get("responsibility"), dict)
               else opened) or {}
        self.ledger.set_meta(self._missing_marker_key(task["id"]), json_dumps({
            "count": count, "last_at": now,
            "responsibility_id": row.get("id"), "route": route,
        }))
        self.ledger.audit("supervisor", "responsibility.missing-detected",
                          "task", task["id"],
                          {"responsibility": row.get("id"), "route": route,
                           "reconciles": count,
                           "created": (opened.get("created")
                                       if isinstance(opened, dict) else None)})
        if target:
            try:
                store.publish_responsibility(
                    row["id"], expected_revision=row["revision"],
                    kind=_RESPONSIBILITY_MISSING_KIND,
                    payload={
                        "classification": "RESPONSIBILITY_MISSING",
                        "summary": ("已托管 Task 仍未完成，且没有有效推进/等待责任；"
                                    "通知队列清空不代表任务已处理"),
                        "task_id": task["id"],
                        "trigger": "bounded-missing-responsibility-check",
                        "reconciles": count,
                        "next_action": row.get("next_action"),
                    },
                    actor="supervisor")
            except LedgerError as e:
                self.ledger.audit("supervisor", "responsibility.missing-publish-rejected",
                                  "responsibility", row.get("id"),
                                  {"code": e.code, "message": str(e)[:300]})
        else:
            self.ledger.add_alert(
                severity="warning", kind="responsibility_unowned",
                title=f"责任无归属：{task['id']}",
                detail=("已托管未完成 Task 缺少可核验协调者，且没有有效责任；"
                        f"task={task['id']} project={task.get('project')} "
                        f"status={task.get('status')}"),
                dedupe_key=f"resp-unowned:{task['id']}",
                task_id=task["id"], attempt_id=current_attempt_id)
        return row

    # ---- 事件入口 -------------------------------------------------
    def handle_stream_item(self, item: dict) -> bool:
        kind = item.get("kind")
        if kind == "connected":
            self.ledger.set_meta("last_connected_at", str(item["at"]))
            return True  # 连接/重连后立即对账，覆盖断流窗口
        if kind == "disconnected":
            self.ledger.set_meta("last_disconnected_at", str(item["at"]))
            self.ledger.set_meta("last_stream_error", str(item.get("error"))[:500])
            return False
        if kind == "error":
            self.ledger.set_meta("last_stream_error", str(item.get("error"))[:500])
            return False
        if kind != "event":
            return False
        envelope = item["event"]
        action = self.ingest_event(envelope)
        # S4：SSE 仅触发观察。执行结束/终端事件让本 session 的等待执行责任在
        # 本轮提前做一次有界 REST 补证；事实仍由 tick_continuation 定点确认，
        # 不把 SSE 事件本身当成本输入已执行证据。
        try:
            etype = str(envelope.get("type") or "")
            if action in ("terminal", "started", "inbox") or "idle" in etype:
                data = envelope.get("data") if isinstance(
                    envelope.get("data"), dict) else {}
                self._promote_responsibilities_for_session(
                    data.get("sessionID") or data.get("sessionId")
                    or envelope.get("sessionID"))
        except Exception as e:
            self.ledger.audit("supervisor", "continuation.sse-promote-error",
                              "event", str(envelope.get("id")),
                              {"error": redact(repr(e), 500)})
        return action in ("terminal", "permission", "form", "retry", "started")

    def ingest_event(self, envelope: dict) -> str:
        ev = classify_event(envelope)
        if ev.get("content"):
            return "ignored"
        etype = envelope.get("type")
        if ev["action"] == "connect":
            return "connect"
        event_id = envelope.get("id")
        if not event_id:
            return "ignored"
        session_id = ev.get("session_id")
        created_ms = envelope.get("created") if isinstance(envelope.get("created"), int) else None
        attempt = self._attempt_for_event(session_id, created_ms)
        durable = envelope.get("durable") or {}
        task_id = attempt["task_id"] if attempt else None
        attempt_id = attempt["id"] if attempt else None
        payload = {
            "type": etype,
            "session_id": session_id,
            "terminal": ev.get("terminal"),
            "reason": ev.get("reason"),
        }
        if ev["action"] == "permission":
            data = envelope.get("data") or {}
            payload.update({
                "permission_id": data.get("id"),
                "action": data.get("action"),
                "resources": data.get("resources"),
            })
        elif ev["action"] == "form":
            data = envelope.get("data") or {}
            payload.update({"form_id": data.get("id"), "title": data.get("title")})
        elif ev["action"] == "retry":
            err = classify_provider_error(ev.get("error"))
            payload["error"] = err
        elif ev["action"] == "terminal":
            data = envelope.get("data") if isinstance(envelope.get("data"), dict) else {}
            if data.get("error"):
                payload["error"] = classify_provider_error(data["error"])
        elif ev["action"] == "inbox":
            data = envelope.get("data") or {}
            payload["inbox_id"] = data.get("inboxID")
        rec = self.ledger.record_event(
            event_id=event_id, type_=etype, session_id=session_id,
            task_id=task_id, attempt_id=attempt_id,
            seq=(durable.get("seq") if isinstance(durable, dict) else None),
            version=(durable.get("version") if isinstance(durable, dict) else None),
            created_ms=created_ms, payload=payload,
        )
        if rec["duplicate"]:
            return "duplicate"
        if rec["stale"]:
            self.ledger.audit("supervisor", "event.stale", "event", event_id,
                              {"attempt": attempt_id, "type": etype})
            return "stale"
        self.ledger.set_meta("last_event_at", str(rec["observed_at"]))

        if ev["action"] == "started":
            if attempt:
                self._on_started(attempt, envelope)
            self._on_coordinator_started(session_id)
            return "started"
        if ev["action"] == "terminal":
            if attempt:
                self.handle_terminal(attempt, ev["terminal"], ev.get("reason"), envelope)
            # 协调者终态统一由 REST 观察路径处理，避免事件/观察双计数
            return "terminal"
        if ev["action"] == "permission":
            if attempt:
                self.handle_permission(attempt, envelope)
            elif session_id:
                self.handle_coordinator_permission(session_id, envelope)
            return "permission"
        if ev["action"] == "permission_replied":
            if attempt:
                self.handle_permission_replied(attempt, session_id)
            return "permission_replied"
        if ev["action"] == "form":
            if attempt:
                self.handle_form(attempt, envelope)
            return "form"
        if ev["action"] == "form_settled":
            if attempt:
                self.handle_permission_replied(attempt, session_id)
            return "form_settled"
        if ev["action"] == "retry":
            err = ev.get("error") or {}
            if attempt:
                self.handle_retry(attempt, envelope, err)
            elif (isinstance(err, dict) and session_id
                  and self.projects_for_coordinator(session_id)):
                # S3：SSE retry 不能只处理 worker attempt；已注册协调者的结构化
                # quota/auth/… 错误计入目标闸门（worker 错误不扩大到其他目标）。
                # observed_at 用事件源时间（稳定执行边界），不用当前 now 配旧键。
                self._observe_coordinator_gate_error(
                    session_id, err,
                    evidence_key=f"sse-retry:{session_id}:{event_id}",
                    observed_at=self.clock.now_ms(),
                    origin_at=created_ms)
            return "retry"
        if ev["action"] == "inbox":
            self.handle_inbox_event(envelope)
            return "inbox"
        if ev["action"] == "progress":
            if attempt:
                now = created_ms or self.clock.now_ms()
                meta = dict(attempt["metadata"])
                if etype == "session.tool.called":
                    data = envelope.get("data") or {}
                    meta["active_tool"] = {
                        "event_id": event_id,
                        "name": data.get("name") or data.get("tool") or "unknown",
                        "tool_id": data.get("id"),
                        "started_at": now,
                    }
                elif etype in ("session.tool.success", "session.tool.failed",
                               "session.step.failed"):
                    meta.pop("active_tool", None)
                self.ledger.update_attempt(
                    attempt["id"],
                    fields={"last_progress_at": now, "last_observed_at": self.clock.now_ms(),
                            "metadata": meta},
                    actor="supervisor", reason=f"event:{etype}",
                )
            return "progress"
        return ev["action"]

    def _attempt_for_event(self, session_id: str | None, created_ms: int | None) -> dict | None:
        """事件 → 尝试归属：open 优先、窗口与输入锚定；不能唯一归属时返回 None。

        多个未结尝试的输入落入同一无法拆分的区间时不按最新 attempt 猜测
        （D2）；事件仍会以 attempt_id=NULL 落库，待后续对账补证。

        同任务后代已完成而前代仍未结时，单凭 session/time 不能把迟到事件
        归到前代。保留未归属事件，由 REST 输入区间补证；跨任务 generation
        不可比较，时间容差也不能产生输入身份。
        """
        if not session_id:
            return None
        attempts = self.ledger.attempts_for_session(session_id, limit=20)
        if not attempts:
            return None
        open_pool = [a for a in attempts if a["status"] not in ATTEMPT_TERMINAL
                     and a["site_released_at"] is None]
        if not open_pool:
            return attempts[0]  # 全部终态：旧事件重放/审计观察
        if len(open_pool) == 1:
            candidate = open_pool[0]
            if any(a["task_id"] == candidate["task_id"]
                   and a["generation"] > candidate["generation"]
                   and (a["metadata"].get("execution") or {}).get("terminal")
                   for a in attempts):
                return None
            return candidate
        # 排队中的输入尚未被执行：不能凭“唯一 inbox 命中的尝试”授予 started/终态
        # 归属（T15 补充：排队 ≠ 已执行）。无法唯一归属时返回 None，调用方仍
        # 记录原始事件（attempt_id=NULL）供重启后由真实执行事实补证。
        return None

    # ---- 执行归属辅助（D2；SSE/REST 同一规则） -----------------------
    def _input_still_queued(self, att: dict) -> bool | None:
        """原派单输入是否仍在 inbox 排队（未执行）。查询失败返回 None。"""
        msg_id = att.get("dispatch_msg_id")
        if not msg_id or not att.get("session_id"):
            return None
        try:
            return any(i.get("id") == msg_id
                       for i in self.client.inbox(att["session_id"]))
        except Exception:
            return None

    def _point_input_message(self, session_id: str, message_id: str) -> dict | None:
        """定点读取原输入并严格校验身份（审查2/审查3）：

        - 返回对象必须是 dict、id == dispatch_msg_id、type == "user"；
        - 若带 sessionID 字段，必须与本尝试 session 一致；
        - time.created 必须为正整数（时间形状校验）。
        任何错 id/type/形状/会话的对象都不是原输入锚点，返回 None
        （调用方按 input-identity-unproven 保持未确认责任）。
        """
        try:
            msg = self.client.get_message(session_id, message_id)
        except Exception:
            return None
        if not isinstance(msg, dict):
            return None
        if msg.get("id") != message_id or msg.get("type") != "user":
            return None
        if msg.get("sessionID") not in (None, session_id):
            return None
        times = msg.get("time")
        t = times.get("created") if isinstance(times, dict) else None
        if not (isinstance(t, int) and not isinstance(t, bool) and t > 0):
            return None
        return msg

    def _input_ownership(self, att: dict, *, input_at=None, ended=None) -> str | None:
        """同 session 是否**其他未结尝试的输入落入本尝试区间**（T16）。

        - 其他尝试输入仍 pending（inbox 排队）→ 未执行，不构成同区间证据；
        - 其他尝试输入锚点可读且 created 在 [input_at, ended] 内 → 共享区间 →
          ambiguous（无法拆分的同区间不按 latest+2s 兜底猜测）；
        - 其他尝试输入已投递但锚点不可读 / 无输入身份 → 无法排除共享 → 保守 ambiguous；
        - 仅当没有其他尝试，或其它输入明确不在本区间 → unique（输入唯一归属）。
        """
        others = [a for a in self.ledger.attempts_for_session(
            att["session_id"] or "", limit=20)
            if a["id"] != att["id"] and a["status"] not in ATTEMPT_TERMINAL
            and a["site_released_at"] is None]
        if not others:
            return "unique"
        for o in others:
            omsg = o.get("dispatch_msg_id")
            if not omsg:
                # 共享 session 的其他未结尝试（attach/无输入）：无法证明不共享区间
                return "ambiguous"
            if self._input_still_queued(o) is True:
                continue  # 该输入尚未执行，不可能落入本区间
            o_session = o.get("session_id") or att.get("session_id")
            o_input_at = None
            try:
                m = self._point_input_message(o_session or "", omsg)
                if m is not None:
                    t = (m.get("time") or {}).get("created")
                    o_input_at = (t if isinstance(t, int) and not isinstance(t, bool)
                                  and t > 0 else None)
            except Exception:
                m = None
            if o_input_at is None:
                # 其他尝试输入已投递但锚点不可读：无法排除共享 → 保守 ambiguous
                return "ambiguous"
            if input_at is not None and ended is not None:
                if input_at <= o_input_at <= ended:
                    return "ambiguous"
                continue
            return "ambiguous"  # 无区间边界信息：保守视为可能共享
        return "unique"

    def _supplement_receipt(self, att, message, reports):
        """只为该代次、该物理输入的实际消费提供归属；不改变责任/业务状态。"""
        mid = message.get("id")
        ref = {"message_id": mid}
        if not mid:
            return ref, "missing-message-id"
        if message.get("sessionID") not in (None, att["session_id"]):
            return ref, "different-execution"
        row = self.ledger.input_for_receipt(att["session_id"], mid)
        if not row or row.get("source") != "managed_input" or row.get("intent") != "execute":
            return ref, "untracked-input"
        tracking = input_rules.tracking_block(row)
        ref.update(input_id=row["input_id"], attempt_id=tracking.get("attempt_id"),
                   generation=tracking.get("generation"))
        if (not tracking.get("tracked") or tracking.get("task_id") != att["task_id"]
                or tracking.get("attempt_id") != att["id"]
                or tracking.get("generation") != att["generation"]):
            return ref, "different-execution"
        if tracking.get("same_candidate") is not True:
            return ref, "same-candidate-not-declared"
        if input_rules.delivery_state(row) != input_rules.DELIVERY_DELIVERED:
            return ref, "delivery-unconfirmed"
        binding = input_rules.continuation_binding(row)
        rid = tracking.get("responsibility_id")
        duty = self._responsibility_store().get_responsibility(rid) if rid else None
        if not duty or not _report_matches(binding, duty):
            return ref, "responsibility-mismatch"
        for created, report in reports.get(mid, ()):
            if created > _message_created(message) and _report_matches(binding, report):
                ref["report_message_id"] = report["source_message_id"]
                return ref, None
        return ref, "missing-bound-report"

    def _rest_interval_evidence(self, att: dict, outcome: str, *, ended_at) -> dict:
        """严格有界区间证明（审查1 + 第三轮）：经组合根 Results.message_window
        复用 C 的 read_message_window 严格分页（覆盖不完整绝不 proven）。

        proven 要求同时满足：
        - coverage.complete 为 True 且原 user 输入锚点命中；
        - 窗口 (input_at, ended] 内**正好一台** idle 消息且匹配 outcome、created == ended；
        - 窗口 (input_at, ended] 内无其他 idle（旧 idle/其他回合夹在锚点与匹配
          idle 之间 → 多回合归属不明，不 proven，不能以 coverage complete 替代
          回合唯一性）且每条后续 user 输入均有同候选声明和精确绑定的消费回执。

        查询失败/覆盖不足/回合不唯一均 proven=False（不 terminal/收集/通知/释放）。
        """
        sid = att.get("session_id")
        msg_id = att.get("dispatch_msg_id")
        if not sid or not msg_id:
            return {}
        execution = (att.get("metadata") or {}).get("execution") or {}
        epoch = execution.get("epoch") or {}
        before = execution.get("session_before") or {}
        b_busy = execution_rules._int(before.get("busy"))
        b_idle = execution_rules._int(before.get("idle"))
        since_ms = (execution_rules._int(epoch.get("started_at"))
                    or b_busy or b_idle)
        ended = execution_rules._int(ended_at)
        if ended is None:
            return {"proven": False, "anchor_id": msg_id, "reason": "missing-end-time"}
        try:
            messages, coverage = self._window(
                sid, anchor_id=msg_id, since_ms=since_ms, until_ms=ended)
        except Exception as e:
            return {"proven": False, "anchor_id": msg_id,
                    "coverage": "query-failed", "error": redact(repr(e), 200)}
        if not coverage.get("complete") or coverage.get("anchor_found") is not True:
            return {"proven": False, "anchor_id": msg_id,
                    "anchor_found": coverage.get("anchor_found") is True,
                    "coverage": _interval_coverage_label(coverage),
                    "reason": coverage.get("reason")}
        anchor = None
        for m in messages:
            if (m.get("id") == msg_id and m.get("type") == "user"
                    and anchor is None):
                anchor = m
        if anchor is None:
            return {"proven": False, "anchor_id": msg_id,
                    "anchor_found": True, "coverage": "complete",
                    "reason": "anchor-identity-mismatch"}
        input_at = _message_created(anchor)
        if input_at <= 0:
            return {"proven": False, "anchor_id": msg_id,
                    "anchor_found": True, "coverage": "complete",
                    "reason": "anchor-time-missing"}
        window = [m for m in messages if input_at <= _message_created(m) <= ended]
        idles = [m for m in window if m.get("type") == "idle"
                 and _message_created(m) > input_at]
        matches = sum(m.get("outcome") == outcome and _message_created(m) == ended
                      for m in idles)
        supplements = [m for m in window if m.get("type") == "user" and m.get("id") != msg_id]
        reports = {}
        if supplements:
            for msg in window:
                if (msg.get("type") != "assistant" or not msg.get("id")
                        or msg.get("sessionID") not in (None, sid)):
                    continue
                parsed = extract_continuation_report([msg], after_ms=input_at, until_ms=ended)
                report = parsed["report"]
                if report and isinstance(report["message_id"], str):
                    reports.setdefault(report["message_id"], []).append((_message_created(msg), report))
        conflicts = len(idles) - min(matches, 1)
        issues, accepted = [], []
        for message in supplements:
            if not execution or _message_created(message) == input_at:
                ref, reason = {"message_id": message.get("id")}, "unproven-anchor-order"
            else:
                ref, reason = self._supplement_receipt(att, message, reports)
            if reason:
                conflicts += 1
                if len(issues) < 8:
                    issues.append(dict(ref, reason=reason))
            elif len(accepted) < 8:
                accepted.append(ref)

        interval = {"anchor_id": msg_id, "anchor_found": True, "coverage": "complete",
                    "idle_count": matches, "has_idle_match": bool(matches),
                    "other_idle_count": len(idles) - matches,
                    "supplement_count": len(supplements), "interval_conflicts": conflicts}
        if matches != 1 or conflicts:
            return dict(interval, proven=False, reason="multi-round-interval", conflicts=issues)
        if accepted:
            interval["consumed_inputs"] = accepted
        return dict(interval, proven=True, input_at=input_at, idle_at=ended,
                    idle_message=True, scanned=len(messages))

    def _window(self, session_id: str, *, anchor_id=None, since_ms=None,
                until_ms=None):
        """经组合根 Results.message_window 共享共享有界读取（预算用 cfg）。"""
        from ..composition import Results
        return Results(self.ledger, lambda: self.client, self.cfg,
                       clock=self.clock).message_window(
            session_id, anchor_id=anchor_id, since_ms=since_ms, until_ms=until_ms)

    def _endpoint_candidate(self, att: dict, *, outcome: str, source: str,
                            ended_at, busy_at=None, event_id: str | None = None,
                            seq: int | None = None) -> dict:
        has_input = bool(att.get("dispatch_msg_id"))
        input_at = None
        if has_input and att.get("session_id"):
            anchor = self._point_input_message(att.get("session_id"),
                                               att.get("dispatch_msg_id"))
            if anchor is not None:
                t = ((anchor.get("time") or {}).get("created"))
                input_at = (t if isinstance(t, int) and not isinstance(t, bool)
                            and t > 0 else None)
        candidate = execution_rules.build_candidate(
            outcome=outcome, source=source, session_id=att["session_id"],
            ended_at=ended_at, busy_at=busy_at, event_id=event_id, seq=seq,
            input_in_inbox=self._input_still_queued(att),
            input_expected=has_input, input_at=input_at)
        execution = (att.get("metadata") or {}).get("execution") or {}
        if not (execution.get("epoch") or {}).get("started_at"):
            candidate["interval"] = self._rest_interval_evidence(
                att, outcome, ended_at=ended_at)
        # 输入归属基于本尝试区间边界（epoch/interval/输入锚点），不用“共享 session
        # 即 ambiguous”的粗粒度判断——支持输入唯一归属（T16 参数化）。
        candidate["input_owner"] = self._input_ownership(
            att, input_at=input_at,
            ended=execution_rules._int(ended_at))
        return candidate

    def _submit_terminal(self, att: dict, *, outcome: str, source: str,
                         event_id: str | None, created_at, busy_at=None,
                         seq: int | None = None,
                         reason: str | None = None) -> dict:
        """统一窄终态提交 + 拒绝时的持久原因与有界唤醒（T02/T03 等）。"""
        candidate = self._endpoint_candidate(
            att, outcome=outcome, source=source, ended_at=created_at,
            busy_at=busy_at, event_id=event_id, seq=seq)
        reason_code = reason or f"session-{outcome}"
        result = self.ledger.record_terminal(
            att["id"], outcome=outcome, source=source, event_id=event_id, seq=seq,
            reason=reason_code, candidate=candidate)
        if result["result"] == "rejected":
            self._notify_unconfirmed(result["attempt"] or att, result["reason"])
        return result

    def _notify_unconfirmed(self, att: dict, reason: str) -> None:
        """拒绝/未确认终态：持久原因 + 去重告警；保留写权与责任。"""
        task = self.ledger.get_task(att["task_id"])
        if task["status"] in ("accepted", "cancelled"):
            return
        unconfirmed = ((att.get("metadata") or {}).get("execution") or {}).get("unconfirmed") or {}
        evidence = unconfirmed.get("evidence") if unconfirmed.get("reason") == reason else {}
        interval = (evidence or {}).get("interval") or {}
        next_action = execution_rules.unconfirmed_next_action(reason, interval)
        self.enqueue_task(
            task, "execution_unconfirmed",
            dedupe_key=f"terminal-unconfirmed:{att['id']}:{reason}",
            payload={
                "task_id": task["id"], "attempt_id": att["id"],
                "session_id": att.get("session_id"),
                "classification": "EXECUTION_UNCONFIRMED",
                "reason": reason,
                **({"interval": interval} if interval else {}),
                "summary": f"终态候选被拒绝（{reason}）；写权保持，等待补证",
                "next_action": next_action,
            },
            priority=6,
        )

    def _recovery_session_before(self, att: dict, *, planned_session: str | None) -> dict | None:
        """恢复路径的派单前快照（未发送状态才可信；仅用于补封存）。

        查询失败/会话缺失返回 None：与 services 派单门一致，不得把“读不到旧回合”
        当成无旧回合继续 seal + prompt。
        """
        sid = planned_session or att.get("session_id")
        if not sid:
            return None
        try:
            snap = self.client.get_session(sid)
        except Exception:
            return None
        if snap is None:
            return None
        times = snap.get("time") or {}

        def _t(key):
            v = times.get(key)
            return int(v) if isinstance(v, int) and v > 0 else None

        return {"outcome": snap.get("outcome"), "idle": _t("idle"),
                "busy": _t("busy"), "updated": _t("updated"),
                "snapshotted_at": self.clock.now_ms(),
                "via": "dispatch-recovery"}

    def _scan_recovery_baseline(self, att: dict) -> dict:
        """F9：恢复路径复用真实前置基线准备（D5），失败抛错由调用方保留尝试。

        经组合根 Results 门面调用扫描适配器，保持 runtime 层不直接依赖 adapters。
        """
        from ..composition import Results
        return Results(self.ledger, lambda: self.client, self.cfg,
                       clock=self.clock).scan_baseline(att["id"])

    def _ensure_attach_epoch(self, att: dict) -> None:
        """F10/T15：no_prompt 附着时仅捕获**正在运行的 busy 区间**为新 epoch。

        历史已完成 busy（有更晚 idle，idle > busy）不能当新执行开始；仅当快照
        没有更晚 idle（busy 之后无 idle）时才附着该 busy。附着不伪造派单消息。
        """
        execution = (att.get("metadata") or {}).get("execution") or {}
        if (execution.get("epoch") or {}).get("started_at"):
            return
        no_prompt = bool((att.get("metadata") or {}).get(
            "dispatch", {}).get("no_prompt"))
        if not no_prompt:
            return
        before = execution.get("session_before") or {}
        busy = execution_rules._int(before.get("busy"))
        idle = execution_rules._int(before.get("idle"))
        if busy is None:
            return  # 无 busy 快照：等待新的执行事实
        if idle is not None and idle > busy:
            return  # 历史已完成 busy（更晚 idle）：不当新 epoch
        self.ledger.record_execution_started(
            att["id"], started_at=busy, event_id=None, source="attach-busy",
            actor="supervisor")

    def _redrive_saved_terminal(self, att: dict) -> None:
        """已记录但待确认的终态事件：归属补齐后补一次提交（T07/重启恢复）。"""
        rows = self.ledger.pending_terminal_events(att["id"])
        for ev in rows:
            outcome = ev.get("terminal")
            if outcome not in ("succeeded", "failed", "interrupted"):
                continue
            if (att.get("metadata") or {}).get("execution", {}).get("terminal", {}).get("outcome"):
                return  # 已有确认终态
            # 重启候选同样不绕过活动/事实冲突门（审查补充）
            conflict = self._activity_conflict(att, ended=ev.get("created_ms"))
            if conflict is not None:
                self._hold_unconfirmed(att, conflict)
                return
            result = self._submit_terminal(
                att, outcome=outcome, source="redrive",
                event_id=ev.get("event_id"), created_at=ev.get("created_ms"),
                reason="redrive-saved-event")
            if result["result"] == "accepted":
                self._after_terminal(result["attempt"], outcome,
                                     reason=None, terminal_error=None)
                return

    # ---- 尝试观察 -------------------------------------------------

    def _activity_conflict(self, att: dict, *, active=None,
                           active_ok: bool = True,
                           ended=None) -> str | None:
        """活动/事实冲突门（T11/审查2 + 第三轮）：刷新 active 与最新 session 后判定。

        返回冲突原因（保留责任，不提交终态/收集/通知/释放）或 None（可推进）。
        - active 查询失败 → activity-unknown（查询异常≠无活动）；
        - 最新 get_session 不可读/缺失 → session-snapshot-unavailable
          （不能以缺失当成已停止而接受终态）；
        - 无论 active 是否包含 worker，都用最新 session 的 busy 对比候选结束：
          更晚 busy（busy > ended）说明候选是更早回合 → newer-busy；
        - active 含 worker 且刷新后仍 active → session-active
          （旧 active 样本刷新为 inactive 时放行，不造成永久等待）。
        """
        fresh_active = active
        fresh_ok = bool(active_ok)
        if active is None:
            try:
                fresh_active = self.client.active_sessions()
                fresh_ok = True
            except Exception:
                fresh_ok = False
        if not fresh_ok:
            # 重试一次刷新：单次瞬时可恢复；仍失败 = 未知
            try:
                fresh_active = self.client.active_sessions()
                fresh_ok = True
            except Exception:
                fresh_ok = False
        if not fresh_ok:
            return "activity-unknown"
        sid = att.get("session_id")
        # 一律读取最新 session 快照：SSE/redrive/REST 与候选 ended 对比
        live = None
        try:
            live = self.client.get_session(sid or "")
        except Exception:
            live = None
        if not isinstance(live, dict) or sid not in (live.get("id"), live.get("sessionID")):
            # 会话不可读/缺失：未知，不得当作“无活动”接受终态
            return "session-snapshot-unavailable"
        lt = live.get("time") or {}
        l_busy = execution_rules._int(lt.get("busy"))
        l_idle = execution_rules._int(lt.get("idle"))
        if sid in (fresh_active or {}):
            if l_busy is not None and (l_idle is None or l_busy > l_idle):
                return execution_rules.R_NEWER_BUSY
            try:
                again = self.client.active_sessions()
            except Exception:
                again = fresh_active
            if sid in (again or {}):
                return "session-active"
            # 旧 active 样本刷新后不再活动：继续用最新 busy 对比候选结束
        if ended is not None:
            end_i = execution_rules._int(ended)
            if l_busy is not None and end_i is not None and l_busy > end_i:
                return execution_rules.R_NEWER_BUSY
        return None

    def _hold_unconfirmed(self, att: dict, reason: str) -> None:
        """持久冲突/unknown 责任：记录 execution.unconfirmed + 审计 + 去重告警。"""
        try:
            self.ledger.record_execution_unconfirmed(
                att["id"], reason=reason, evidence={"activity_hold": True},
                actor="supervisor")
        except Exception:
            pass
        try:
            self.ledger.audit(
                "supervisor", "terminal.rejected", "attempt", att["id"],
                {"reason": reason, "classification": "activity-conflict",
                 "held": True})
        except Exception:
            pass
        self._notify_unconfirmed(att, reason)

    def observe_attempt(self, att: dict, active: dict[str, dict],
                        *, active_ok: bool = True) -> None:
        if att["dispatch_state"] != "confirmed":
            att = self.reconcile_dispatch(att)
            if att is None:
                return
        if not att.get("session_id"):
            return
        try:
            session = self.client.get_session(att["session_id"])
        except ApiError as e:
            if e.status == 404:
                self.mark_attempt_lost(att, "session-not-found")
                return
            raise
        if session is None:
            self.mark_attempt_lost(att, "session-not-found")
            return

        now = self.clock.now_ms()
        times = session.get("time") or {}
        session_meta = {
            "outcome": session.get("outcome"),
            "agent": session.get("agent"),
            "model": session.get("model"),
            "title": session.get("title"),
            "time": times,
            "location": session.get("location"),
        }
        fields: dict[str, Any] = {
            "last_observed_at": now,
            "session_meta": session_meta,
        }
        progress = int(times.get("updated") or times.get("busy") or now)
        if progress > (att.get("last_progress_at") or 0):
            fields["last_progress_at"] = progress
        self.ledger.update_attempt(att["id"], fields=fields, actor="supervisor",
                                   reason="observe")

        outcome = session.get("outcome")
        if outcome in ("succeeded", "failed", "interrupted") and not att.get("terminal_at"):
            # 活动/事实冲突门（T11/审查2）：使用传入的 active/active_ok 快照并刷新
            # active/session 事实；活动冲突或查询失败不得提交终态/收集/通知/释放。
            conflict = self._activity_conflict(att, active=active, active_ok=active_ok,
                                               ended=times.get("idle"))
            if conflict is None:
                # 统一归属：REST outcome/idle 只是候选，必须经窄终态事务校验（D2/D3）
                idled = times.get("idle")
                result = self._submit_terminal(
                    att, outcome=outcome, source="rest",
                    event_id="obs-" + str(idled if isinstance(idled, int)
                                          else self.clock.now_ms()),
                    created_at=idled, busy_at=times.get("busy"))
                if result["result"] == "accepted":
                    self._after_terminal(result["attempt"], outcome, reason=None,
                                         terminal_error=None)
                    return
                att = self.ledger.get_attempt(att["id"])
                if att.get("terminal_at"):
                    return
                # 候选被拒：unconfirmed 持久原因 + 有界唤醒已记录；重试已保存事件
                self._redrive_saved_terminal(att)
                att = self.ledger.get_attempt(att["id"])
                if att.get("terminal_at"):
                    return
            else:
                # 冲突/unknown：持久原因 + 去重告警，写权与责任保持
                self._hold_unconfirmed(att, conflict)
        # 终态已确定：晚到观察不得把尝试降级回 idle/stall 或改写终态
        # （G4：旧代次晚到事实不覆盖当前尝试归属与人工意图）。
        if att.get("terminal_at"):
            return

        is_active = att["session_id"] in active
        if not active_ok:
            # active 集合不可用：只做终态/权限/表单观察，避免误判 idle/stall
            return

        # 权限/表单等待：即使 SSE 漏事件，REST 也能发现
        pending = []
        try:
            pending = [p for p in self.client.permissions(att["directory"])
                       if p.get("sessionID") == att["session_id"]]
        except Exception:
            pass
        if pending:
            if att["status"] not in ("blocked_permission", "terminal_succeeded",
                                     "terminal_failed", "terminal_interrupted"):
                att = self.ledger.transition_attempt(
                    att["id"], "blocked_permission", reason="permission-pending",
                    actor="supervisor",
                )
            for p in pending:
                self.enqueue_permission(att, p)
            return
        if att["status"] == "blocked_permission" and is_active:
            att = self.ledger.transition_attempt(att["id"], "running",
                                                 reason="permission-resolved",
                                                 actor="supervisor")

        try:
            forms = self.client.forms(att["session_id"])
        except Exception:
            forms = []
        if forms:
            if att["status"] not in ("blocked_form", "terminal_succeeded",
                                     "terminal_failed", "terminal_interrupted"):
                att = self.ledger.transition_attempt(
                    att["id"], "blocked_form", reason="form-pending", actor="supervisor",
                )
            for f in forms:
                self.enqueue_form(att, f)
            return
        if att["status"] == "blocked_form" and is_active:
            att = self.ledger.transition_attempt(att["id"], "running",
                                                 reason="form-resolved", actor="supervisor")

        if is_active:
            if att["status"] in ("idle_unconfirmed", "provider_retry", "stalled",
                                 "dispatch_unknown", "dispatched", "dispatching", "creating"):
                att = self.ledger.transition_attempt(att["id"], "running",
                                                     reason="session-active",
                                                     actor="supervisor")
            self.evaluate_stall(att, session)
            self.refresh_side_effects(att)
            return

        # 不活跃且没有终态：区分 pending inbox / 暂时 idle / 真停滞
        try:
            inbox = self.client.inbox(att["session_id"])
        except Exception:
            inbox = []
        if inbox:
            fresh = self.ledger.get_attempt(att["id"])
            self.ledger.update_attempt(
                att["id"],
                fields={"metadata": {**fresh["metadata"], "pending_inbox": len(inbox)}},
                actor="supervisor", reason="inbox-pending",
            )
            idle_since = int(times.get("idle") or times.get("updated") or now)
            if now - idle_since > int(self.cfg["dispatch_unknown_grace"]) * 1000:
                self.enqueue_idle(att, "inbox-pending")
            return
        idle_since = int(times.get("idle") or times.get("updated") or now)
        if att["status"] not in ("idle_unconfirmed", "stalled"):
            att = self.ledger.transition_attempt(
                att["id"], "idle_unconfirmed", reason="idle-without-outcome",
                actor="supervisor",
            )
        if now - idle_since > int(self.cfg["dispatch_unknown_grace"]) * 1000:
            self.enqueue_idle(att, "idle-without-outcome")
        self.evaluate_stall(att, session)

    def _dispatch_recovery_allowed(self, attempt_id: str, *, write: bool = False) -> bool:
        att = self.ledger.get_attempt(attempt_id)
        task = self.ledger.get_task(att["task_id"])
        if (task["current_attempt_id"] != attempt_id
                or task["status"] in ("paused", "cancelled", "accepted")
                or task.get("closed_at") is not None
                or att["status"] in ATTEMPT_TERMINAL
                or att["status"] == "stop_requested"
                or att["stop_requested_at"] is not None
                or att["site_released_at"] is not None):
            return False
        if write:
            try:
                self.ledger.check_attempt_dispatch(attempt_id)
            except LedgerError:
                return False
            retry = att["metadata"].get("dispatch_retry") or {}
            if (retry.get("count", 0) >= int(self.cfg["dispatch_max_retries"])
                    or retry.get("next_at", 0) > self.clock.now_ms()):
                return False
        return True

    def reconcile_dispatch(self, att: dict) -> dict | None:
        """派单结果未知：先查 session/inbox/messages，绝不换 ID 重投。"""
        att = self.ledger.get_attempt(att["id"])
        if not self._dispatch_recovery_allowed(att["id"]):
            return None
        if att["dispatch_state"] == "confirmed":
            return att
        # 恢复同样执行基线门禁（D1）：任何可能的发送之前完成封存。
        # 已封存幂等；请求可能已发出（dispatch_unknown）时不重新解释基线，
        # 用占位标记诚实地记录"无法取得可靠的派单前快照"。
        meta = dict(att["metadata"])
        dispatch = meta.get("dispatch") or {}
        no_prompt = bool(dispatch.get("no_prompt"))
        create = dispatch.get("create") or {}
        msg_id = att.get("dispatch_msg_id")
        planned_session = att.get("session_id") or dispatch.get("session_id_planned")
        execution_meta = (att["metadata"].get("execution") or {})
        if not (execution_meta.get("dispatch") or {}).get("sealed_at"):
            if att["dispatch_state"] == "unknown":
                # 请求可能已发出：不能重建派单前基线，保留占位标记诚实记录。
                session_before = {"post_send_recovery": True}
                baseline_ref: dict = {}
            else:
                # recovery 未发送状态（prepared 等）：基线封存前崩溃。恢复路径
                # 必须复用真实前置基线准备（D1/D5/F9）：扫描声明交付物，失败则
                # 不 seal、不发送 prompt。
                session_before = self._recovery_session_before(
                    att, planned_session=planned_session)
                if session_before is None:
                    if no_prompt:
                        # attach 不能对不存在的 session 发明归属：保持待补证不发送
                        self.ledger.audit(
                            "supervisor", "recovery.attach-target-unavailable",
                            "attempt", att["id"],
                            {"reason": "attach-session-missing"})
                        return None
                    exists = False
                    if planned_session:
                        try:
                            exists = self.client.get_session(planned_session) is not None
                        except Exception:
                            exists = False
                    if bool(create) and not exists:
                        # 全新 prompt session：无旧回合来自身份（与 services mode=new 一致）
                        session_before = {}
                    else:
                        self.ledger.audit(
                            "supervisor", "recovery.session-before-unavailable",
                            "attempt", att["id"],
                            {"reason": "pre-send-snapshot-unreadable"})
                        return None  # 查不到派单前旧回合：不能假装无旧回合
                try:
                    baseline_ref = self._scan_recovery_baseline(att)
                except Exception as e:
                    self.ledger.audit("supervisor", "recovery.baseline-failed",
                                      "attempt", att["id"],
                                      {"error": redact(repr(e), 300)})
                    return None  # 基线准备失败：保留尝试，不发送 prompt
            try:
                self.ledger.seal_execution(
                    att["id"], mode=("attach" if no_prompt else "reuse"),
                    session_before=session_before, baseline=baseline_ref,
                    allow_send=True,
                    actor="supervisor", reason="dispatch-recovery-seal")
            except LedgerError:
                return None  # 人工控制/非当前：恢复路径不越权
            att = self.ledger.get_attempt(att["id"])
            meta = dict(att["metadata"])
            dispatch = meta.get("dispatch") or {}
            no_prompt = bool(dispatch.get("no_prompt"))
            create = dispatch.get("create") or {}
            msg_id = att.get("dispatch_msg_id")
            planned_session = att.get("session_id") or dispatch.get("session_id_planned")
        prompt_text = dispatch.get("prompt")

        if not planned_session:
            self.mark_attempt_lost(att, "no-planned-session")
            return None

        # 1) session 创建结果未知：同 ID 查询/创建，避免两个可写执行者
        session = None
        try:
            session = self.client.get_session(planned_session)
        except ApiError as e:
            if e.status != 404:
                raise
        if session is None:
            if not create:
                self.mark_attempt_lost(att, "session-missing-no-create-spec")
                return None
            if not self._dispatch_recovery_allowed(att["id"], write=True):
                return None
            try:
                session = self.client.create_session(
                    session_id=planned_session,
                    directory=create.get("directory") or att["directory"],
                    title=create.get("title") or f"oc-orch {att['task_id']} {att['id']}",
                    agent=create.get("agent"),
                    model=create.get("model"),
                    metadata=create.get("metadata") or {},
                    permissions=create.get("permissions"),
                )
            except ApiError as e:
                if e.status == 409:
                    session = self.client.get_session(planned_session)
                else:
                    self._dispatch_retry(att, f"create-session:{e}")
                    return None
            except TransportError as e:
                self._dispatch_retry(att, f"create-session:{e}", transport=True)
                return None
        if session is None:
            self._dispatch_retry(att, "create-session-unknown")
            return None
        if att["session_id"] != planned_session:
            self.ledger.update_attempt(att["id"], fields={"session_id": planned_session},
                                       actor="supervisor", reason="dispatch-reconcile")
        att = self.ledger.record_dispatch(att["id"], "session_created", actor="supervisor",
                                          reason="dispatch-reconcile")
        if not self._dispatch_recovery_allowed(att["id"]):
            return None

        # 1.5) no_prompt 附件（--no-prompt attach）：确认/创建原 session 后直接记为
        #      confirmed/dispatched，不允许 prompt。否则崩溃窗口可能把附件变成真派单。
        if dispatch.get("no_prompt"):
            confirmed = self.ledger.record_dispatch(
                att["id"], "confirmed", actor="supervisor",
                task_status="dispatched", reason="dispatch-confirmed-no-prompt")
            self._ensure_attach_epoch(confirmed)
            return confirmed

        # 2) prompt 是否已受理：inbox 或 messages 中查同 ID
        admitted = False
        if msg_id:
            try:
                admitted = any(i.get("id") == msg_id for i in self.client.inbox(planned_session))
            except Exception:
                pass
            if not admitted:
                # 原 ID 定点确认（不能用最近 80 条猜测后重发已受理历史消息）；
                # 返回对象必须严格匹配输入身份，坏响应不能当“已受理”而停投。
                admitted = self._point_input_message(planned_session, msg_id) is not None
        if admitted:
            return self.ledger.record_dispatch(att["id"], "confirmed", actor="supervisor",
                                               task_status="dispatched", reason="dispatch-confirmed-by-inbox")

        if not prompt_text or not msg_id:
            self.mark_attempt_lost(att, "dispatch-missing-prompt")
            return None
        if not self._dispatch_recovery_allowed(att["id"], write=True):
            return None
        from ..application.delivery_action import authorize_attempt

        try:
            authorize_attempt(self.ledger, self.client, self.clock, att["id"])
        except LedgerError as exc:
            self.ledger.add_alert(severity="high", kind="delivery_policy_blocked",
                title="派单恢复仅保原身份对账，资源许可未取得", detail=str(exc),
                dedupe_key=f"delivery-policy:{att['id']}:{exc.code}",
                task_id=att["task_id"], attempt_id=att["id"])
            return None
        try:
            self.client.prompt(
                planned_session, message_id=msg_id, text=prompt_text,
                delivery="queue",
                metadata={"oc_orch_task": att["task_id"],
                          "oc_orch_attempt": att["id"],
                          "oc_orch_generation": att["generation"]},
            )
        except ApiError as e:
            if e.status == 409:
                # 冲突：再查一次，确认是否已受理
                try:
                    msgs = self.client.messages(planned_session, limit=80, order="desc")
                    if any(m.get("id") == msg_id for m in msgs):
                        return self.ledger.record_dispatch(
                            att["id"], "confirmed", actor="supervisor", task_status="dispatched",
                            reason="dispatch-confirmed-after-conflict")
                except Exception:
                    pass
            self._dispatch_retry(att, f"prompt:{e}")
            return None
        except TransportError as e:
            self._dispatch_retry(att, f"prompt:{e}", transport=True)
            return None
        return self.ledger.record_dispatch(att["id"], "confirmed", actor="supervisor",
                                           started=True, task_status="dispatched", reason="dispatch-confirmed")

    def mark_attempt_lost(self, att: dict, reason: str) -> None:
        if att["status"] in ATTEMPT_TERMINAL:
            return
        self.ledger.transition_attempt(att["id"], "lost", reason=reason,
                                       actor="supervisor",
                                       extra={"terminal_reason": reason})
        task = self.ledger.get_task(att["task_id"])
        self.enqueue_task(
            task, "attempt_lost",
            dedupe_key=f"attempt-lost:{att['id']}",
            payload={
                "task_id": task["id"], "attempt_id": att["id"],
                "session_id": att.get("session_id"),
                "classification": "SESSION_LOST",
                "summary": f"执行 session 已不可发现（{reason}）；不能当作任务完成",
                "next_action": "核对是否被删除/服务重建；需要新尝试时显式 rework，不换 ID 重投",
            },
            priority=9,
        )

    def _dispatch_retry(self, att: dict, error: str, *, transport: bool = False) -> None:
        att = self.ledger.record_dispatch(att["id"], "unknown", actor="supervisor",
                                          reason="dispatch-retry")
        if att["dispatch_state"] == "confirmed" or not self._dispatch_recovery_allowed(att["id"]):
            return
        meta = dict(att["metadata"])
        retry = meta.get("dispatch_retry") or {"count": 0, "first_at": self.clock.now_ms()}
        retry["count"] = retry.get("count", 0) + 1
        retry["last_at"] = self.clock.now_ms()
        retry["last_error"] = redact(error, 500)
        meta["dispatch_retry"] = retry
        next_at = self.clock.now_ms() + min(300, 15 * (2 ** min(retry["count"], 4))) * 1000
        retry["next_at"] = next_at
        self.ledger.update_attempt(
            att["id"],
            fields={"metadata": meta, "last_observed_at": self.clock.now_ms()},
            actor="supervisor", reason="dispatch-retry",
        )
        if retry["count"] >= int(self.cfg["dispatch_max_retries"]):
            task = self.ledger.get_task(att["task_id"])
            target = self.coordinator_for(task)
            payload = {
                "task_id": task["id"], "attempt_id": att["id"],
                "session_id": att.get("session_id") or meta.get("dispatch", {}).get(
                    "session_id_planned"),
                "classification": "DISPATCH_UNKNOWN",
                "summary": f"派单重试 {retry['count']} 次仍无法确认受理；不得换 ID 重投",
                "last_error": retry["last_error"],
                "next_action": "查询原 session/inbox/关联回执；人工决定是否释放原尝试",
            }
            if target:
                self.ledger.enqueue_notification(
                    kind="dispatch_unknown", target_session=target,
                    dedupe_key=f"dispatch-unknown:{att['id']}",
                    payload=payload, project=task["project"], task_id=task["id"],
                    attempt_id=att["id"], priority=8, max_attempts=3,
                    next_attempt_at=next_at,
                )
            else:
                self.ledger.add_alert(
                    severity="high", kind="dispatch_unknown",
                    title=f"派单结果未知：{task['id']}",
                    detail=json_dumps(payload, ensure_ascii=False),
                    dedupe_key=f"dispatch-unknown:{att['id']}",
                    task_id=task["id"], attempt_id=att["id"],
                )

    # ---- 终态/结果 -------------------------------------------------
    def handle_terminal(self, att: dict, outcome: str, reason: str | None,
                        envelope: dict) -> None:
        att = self.ledger.get_attempt(att["id"])
        if att.get("terminal_at"):
            return
        mapping = {
            "succeeded": "terminal_succeeded",
            "failed": "terminal_failed",
            "interrupted": "terminal_interrupted",
        }
        if mapping.get(outcome) is None:
            return
        created = envelope.get("created") if isinstance(envelope.get("created"), int) else None
        event_id = envelope.get("id")
        durable = envelope.get("durable") if isinstance(envelope.get("durable"), dict) else {}
        data = envelope.get("data") if isinstance(envelope.get("data"), dict) else {}
        err = classify_provider_error(data.get("error")) if data.get("error") else None
        # SSE 候选同样不绕过活动/事实冲突门（审查补充）：刷新 active/session
        conflict = self._activity_conflict(att, ended=created)
        if conflict is not None:
            self._hold_unconfirmed(att, conflict)
            return
        result = self._submit_terminal(
            att, outcome=outcome, source="sse", event_id=event_id,
            created_at=created, seq=durable.get("seq"),
            reason=reason or f"session-{outcome}")
        if result["result"] != "accepted":
            return  # rejected：持久原因与有界唤醒已在提交内处理；duplicate：finalize 补责任
        self._after_terminal(result["attempt"], outcome, reason=reason,
                             terminal_error=err)

    def _after_terminal(self, att: dict, outcome: str, *, reason: str | None,
                        terminal_error: dict | None) -> None:
        """终态被接受后的统一收尾：错误证据、写权释放门、结果责任。"""
        if terminal_error:
            meta = dict(att["metadata"])
            meta["terminal_error"] = terminal_error
            att = self.ledger.update_attempt(
                att["id"], fields={"metadata": meta}, actor="supervisor",
                reason="terminal-error-evidence")
        task = self.ledger.get_task(att["task_id"])
        # 人工控制/已关闭任务：晚到执行事实可审计，但不产生正常成功推进通知（D9 抑制）
        task_controlled = (task["status"] in ("paused", "cancelled", "accepted")
                           or task.get("closed_at") is not None)

        # 写权释放门（D4）：执行归属已确认，还需外部活动三态全部 inactive；
        # 任一 unknown 保留写权（查询异常不能当作无副作用）。
        side = self.refresh_side_effects(att)
        if side["all_stopped"]:
            self.ledger.release_site(
                att["id"], confirmed=True, external_stopped=True,
                actor="supervisor", note=f"terminal-{outcome}-side-effects-inactive",
            )
        elif side["has_side_effects"]:
            self.enqueue_side_effects(att, side)
        else:
            self._notify_unconfirmed(att, "side-effects-unknown")

        if outcome == "succeeded":
            model = self._attempt_model(att)
            if model:
                self.ledger.clear_provider_state(model, reason="worker-succeeded")
            if self.cfg["auto_collect"] and not task_controlled:
                # 共享收集用例（D7/D9）：正式结果与通知责任由窄事务原子提交；
                # Supervisor 不再自行拼装 result_ready payload。
                try:
                    collect_attempt(self.ledger, self.client, att["id"], self.cfg,
                                    auto=True, clock=self.clock.now_ms)
                except Exception as e:
                    self._collect_failure(task, att, e)
            elif not task_controlled:
                # D9：auto_collect=false → COLLECTION_REQUIRED（不写无版本 RESULT_READY）
                self.enqueue_task(
                    task, "execution_succeeded",
                    dedupe_key=f"execution-succeeded:{att['id']}",
                    payload={
                        "task_id": task["id"], "attempt_id": att["id"],
                        "session_id": att["session_id"],
                        "classification": "COLLECTION_REQUIRED",
                        "summary": "执行回合成功结束；尚未自动收集结果；执行人工 collect 后核验交付物",
                        "next_action": ("执行 collect（返回结果版本与通知 ID，"
                                        "不隐式 ACK）；再按验收标准 verify"),
                    },
                    priority=6,
                )
            else:
                self.ledger.audit(
                    "supervisor", "notification.suppressed-manual-control",
                    "attempt", att["id"],
                    {"kind": "execution_succeeded",
                     "task_status": task["status"]})
        elif outcome == "failed":
            meta = att["metadata"].get("provider_retry") or {}
            self.enqueue_task(
                task, "execution_failed",
                dedupe_key=f"execution-failed:{att['id']}",
                payload={
                    "task_id": task["id"], "attempt_id": att["id"],
                    "session_id": att["session_id"],
                    "classification": "EXECUTION_FAILED",
                    "summary": "执行回合失败；不自动无限重试，也不自动替换执行者",
                    "provider_error": meta.get("last_error"),
                    "next_action": "判断 429/403/额度/模型问题；按授权决定补上下文、换模型或换现场",
                },
                priority=8,
            )
            # 提供商硬故障/预算耗尽：登记故障恢复队列（自动切换须另行授权）。
            self.observe_fault_failure(task=task, att=att, terminal_error=terminal_error)
        else:
            self.enqueue_task(
                task, "execution_interrupted",
                dedupe_key=f"execution-interrupted:{att['id']}",
                payload={
                    "task_id": task["id"], "attempt_id": att["id"],
                    "session_id": att["session_id"],
                    "classification": "INTERRUPTED",
                    "summary": f"执行被中断（reason={reason or 'unknown'}）；保留人工意图，不自动复活",
                    "next_action": "确认是否人工停止；如继续，需显式 rework/新尝试",
                },
                priority=9,
            )

    def _collect_failure(self, task: dict, att: dict, error: Exception) -> None:
        """收集失败：有界重试预算（D7）；耗尽转 result_missing + 一次告警通知。"""
        reason = f"collect-error:{type(error).__name__}:{redact(repr(error), 200)}"
        try:
            pending = self.ledger.record_collection_pending(
                att["id"], reason=reason,
                next_collect_at=self.clock.now_ms()
                + int(self.cfg["collect_retry_interval"]) * 1000,
                max_attempts=int(self.cfg["collect_max_attempts"]),
                interval_ms=int(self.cfg["collect_retry_interval"]) * 1000,
                actor="supervisor")
        except LedgerError:
            pending = {"exhausted": False}
        if pending.get("exhausted"):
            self.enqueue_task(
                task, "result_collect_failed",
                dedupe_key=f"collect-failed:{att['id']}",
                payload={
                    "task_id": task["id"], "attempt_id": att["id"],
                    "classification": "COLLECT_FAILED",
                    "summary": f"自动收集重试耗尽，仍无法取得结果：{reason}",
                    "next_action": "人工读取 session 消息与现场文件；"
                                   "显式 collect 补证或显式 fail；不要直接 accepted",
                },
                priority=8,
            )
        else:
            self.ledger.audit("supervisor", "collect.error", "attempt", att["id"],
                              {"error": redact(repr(error), 400),
                               "retry": "scheduled"})

    def finalize_terminals(self, active: dict[str, dict]) -> None:
        """补收集与写权确认：keyset 分页 + 持久预算门（T24/T33）。"""
        now = self.clock.now_ms()
        if self.cfg["auto_collect"]:
            after_id = ""
            while True:
                page = self.ledger.attempts_after(status="terminal_succeeded",
                                                  after_id=after_id, limit=100)
                if not page:
                    break
                after_id = page[-1]["id"]
                for att in page:
                    self._maybe_recollect(att, now)
        # 终态但未释放写权的：三态检查后释放（keyset 分页防饥饿）
        after_id = ""
        while True:
            page = self.ledger.attempts_after(after_id=after_id, limit=100)
            if not page:
                break
            after_id = page[-1]["id"]
            for att in page:
                if att["status"] not in ATTEMPT_TERMINAL or att.get("site_released_at"):
                    continue
                # D4：accepted/rejected/cancelled/lost 等业务终态也不能单独证明外部
                # 执行已停止（审查 F8）；一律经 session/shell/子 session 三态检查。
                side = self.refresh_side_effects(att)
                if side["all_stopped"]:
                    self.ledger.release_site(
                        att["id"], confirmed=True, external_stopped=True,
                        actor="supervisor", note="terminal-side-effects-inactive",
                    )
                elif side["has_side_effects"]:
                    self.enqueue_side_effects(att, side)
                elif side["unknown"]:
                    # 查询失败≠无副作用：保留写权，去重告警待复核（D4）
                    self._notify_unconfirmed(att, "side-effects-unknown")

    def _maybe_recollect(self, att: dict, now: int) -> None:
        """按持久预算/到期时间决定是否补收集；耗尽后不再无界重试。"""
        if att.get("result_collected_at"):
            return
        collection = (att["metadata"] or {}).get("collection") or {}
        if collection.get("state") in ("exhausted", "complete"):
            return
        next_at = collection.get("next_collect_at")
        if isinstance(next_at, int) and next_at > now:
            return
        task = self.ledger.get_task(att["task_id"])
        if task["status"] in ("paused", "cancelled", "accepted") or task.get(
                "closed_at") is not None:
            return
        try:
            collect_attempt(self.ledger, self.client, att["id"], self.cfg,
                            auto=True, clock=self.clock.now_ms)
        except Exception as e:
            self._collect_failure(task, self.ledger.get_attempt(att["id"]), e)

    def refresh_side_effects(self, att: dict) -> dict:
        """外部活动三态检查（D4）：inactive/active/unknown + 观测时间与原因。

        - session：active 集合 + 最新 get_session 的 busy/idle 双证（/active 缺席
          但 session 最近仍在 busy 也算活动）；
        - shells：接口按 directory 返回；任何 running shell（无法归属/无 cwd/
          子路径）一律保守视为活动，不得当作 inactive；
        - children：分页上限达到即 unknown（第 51 个活动子 session 不可漏）；
        - 持久化经 Ledger.record_side_effect_observation 窄 merge，只合并观察
          子字段，保留并发 execution/collection/stop 事实（不整块覆盖 metadata）。

        查询失败是 unknown 而不是"无活动"；只有归属确认且全部必要事实 inactive
        才允许自动 release（释放门在使用方）。
        """
        now = self.clock.now_ms()
        checks: dict[str, dict] = {}
        # 1) 本 session 是否活动：active 集合 + get_session busy/idle 双证
        try:
            active = self.client.active_sessions()
            state = "active" if att.get("session_id") in active else "inactive"
            try:
                snap = self.client.get_session(att["session_id"])
            except Exception:
                snap = None
            if snap is not None:
                t = snap.get("time") or {}
                b = execution_rules._int(t.get("busy"))
                i = execution_rules._int(t.get("idle"))
                if b is not None and (i is None or b > i):
                    state = "active"  # /active 未列出但 session 最近仍在 busy
            checks["session"] = {"state": state, "at": now}
        except Exception as e:
            checks["session"] = {"state": "unknown", "at": now,
                                 "error": redact(repr(e), 200)}
        # 2) 目录内后台 shell：无法归属的 running shell 保守视为活动
        running_shells: list[dict] = []
        try:
            for sh in self.client.shells(att["directory"]):
                if sh.get("status") != "running":
                    continue
                meta = sh.get("metadata") or {}
                sid = meta.get("sessionID") or meta.get("sessionId")
                if sid == att["session_id"]:
                    running_shells.append(sh)
                else:
                    # 无 sessionID/异 sessionID 且 cwd 缺失或子路径：不能当 inactive
                    running_shells.append({**sh, "unattributed": True})
            checks["shells"] = {
                "state": "active" if running_shells else "inactive", "at": now,
                "items": [
                    {"id": s.get("id"), "status": s.get("status"),
                     "unattributed": bool(s.get("unattributed"))}
                    for s in running_shells
                ] if running_shells else [],
            }
        except Exception as e:
            checks["shells"] = {"state": "unknown", "at": now,
                                "error": redact(repr(e), 200)}
        # 3) 活动子 session：分页覆盖；达到 limit 即 unknown（第 51 个不可漏）
        active_children: list[dict] = []
        try:
            children_cap = "complete"
            if att.get("session_id"):
                limit = 50
                children = self.client.list_sessions(parent_id=att["session_id"],
                                                     limit=limit)
                act = self.client.active_sessions()
                active_children = [c for c in children if c.get("id") in act]
                if len(children) >= limit:
                    children_cap = "capped"
            checks["children"] = {
                "state": ("active" if active_children else
                          "unknown" if children_cap == "capped" else "inactive"),
                "coverage": children_cap, "at": now,
                "items": [c.get("id") for c in active_children],
            }
        except Exception as e:
            checks["children"] = {"state": "unknown", "at": now,
                                  "error": redact(repr(e), 200)}
        # 持久化：窄 merge 只合并观察子字段（保留并发 execution/collection/stop）
        observation = {
            "active_shells": checks["shells"].get("items") or [],
            "active_children": checks["children"].get("items") or [],
            "side_effects_checked_at": now,
            "side_effect_checks": {k: {"state": v["state"], "at": v.get("at")}
                                   for k, v in checks.items()},
        }
        try:
            self.ledger.record_side_effect_observation(
                att["id"], observation=observation, actor="supervisor")
        except Exception:
            pass  # 观察持久化失败不阻断释放门判定；下轮重试
        all_stopped = all(v["state"] == "inactive" for v in checks.values())
        has_side_effects = any(v["state"] == "active" for v in checks.values()) or bool(
            running_shells or active_children)
        return {
            "checks": checks,
            "running_shells": running_shells,
            "active_children": active_children,
            "all_stopped": all_stopped,
            "has_side_effects": has_side_effects,
            "unknown": {k: v for k, v in checks.items() if v["state"] == "unknown"},
        }

    def enqueue_side_effects(self, att: dict, side: dict) -> None:
        task = self.ledger.get_task(att["task_id"])
        self.enqueue_task(
            task, "side_effects_running",
            dedupe_key=f"sidefx:{att['id']}",
            payload={
                "task_id": task["id"], "attempt_id": att["id"],
                "session_id": att["session_id"],
                "classification": "SIDE_EFFECTS_RUNNING",
                "summary": "执行回合已结束，但仍有后台 shell/子 session；写现场未释放",
                "running_shells": [
                    {"id": s.get("id"), "unattributed": bool(s.get("unattributed"))}
                    for s in side["running_shells"]
                ],
                "active_children": [c.get("id") for c in side["active_children"]],
                "next_action": "等待后台结束；确认停止后执行 attempt release --confirm",
            },
            priority=7,
        )

    def evaluate_stall(self, att: dict, session: dict) -> None:
        now = self.clock.now_ms()
        times = session.get("time") or {}
        last = max(int(att.get("last_progress_at") or 0),
                   int(times.get("updated") or 0))
        task = self.ledger.get_task(att["task_id"])
        stall_after = int(att.get("stall_after") or
                          task["metadata"].get("stall_after") or
                          self.cfg["stall_after"])
        deadline = att.get("deadline_at")
        overdue = bool(deadline and now > int(deadline))
        started = att.get("started_at") or att.get("dispatched_at") or att.get("created_at")
        if (started and not overdue
                and now - int(started) < int(self.cfg["startup_grace"]) * 1000):
            # 模型冷启动/排队不属于“无有效进展”
            if att["status"] == "stalled":
                self.ledger.transition_attempt(att["id"], "running",
                                               reason="startup-grace",
                                               actor="supervisor")
            return
        tool = att["metadata"].get("active_tool") or {}
        tool_started = int(tool.get("started_at") or 0)
        tool_grace = int(self.cfg["tool_expected_grace"]) * 1000
        tool_running = bool(tool_started and now - tool_started < tool_grace)
        if tool_running and not overdue:
            if att["status"] == "stalled":
                self.ledger.transition_attempt(att["id"], "running",
                                               reason="tool-still-running",
                                               actor="supervisor")
            return
        no_progress = bool(last and now - last > stall_after * 1000)
        if not (overdue or no_progress):
            if att["status"] == "stalled":
                self.ledger.transition_attempt(att["id"], "running",
                                               reason="progress-resumed",
                                               actor="supervisor")
            return
        if att["status"] not in ("stalled",):
            self.ledger.transition_attempt(
                att["id"], "stalled", reason="deadline" if overdue else "no-progress",
                actor="supervisor",
                extra={"metadata": {**att["metadata"],
                                    "stall_since": now,
                                    "stall_last_progress": last}},
            )
        classification = ("DEADLINE_EXCEEDED" if overdue else
                          ("LONG_TOOL_OVERRUN" if tool_started else "NO_PROGRESS"))
        bucket = last // max(1, stall_after * 1000 // 2)
        self.enqueue_task(
            task, "stall",
            dedupe_key=f"stall:{att['id']}:{classification}:{bucket}",
            payload={
                "task_id": task["id"], "attempt_id": att["id"],
                "session_id": att["session_id"],
                "classification": classification,
                "summary": (
                    f"无有效进展已 {(now - last) // 1000}s（阈值 {stall_after}s）"
                    if not overdue else "超过任务显式期限"
                ),
                "active_tool": tool or None,
                "last_progress_at": last,
                "deadline_at": deadline,
                "next_action": (
                    "检查日志/进程/外部操作；不要直接判失败；"
                    "需要停止时先 attempt stop，确认后再释放现场"
                ),
                "needed_from": "coordinator",
            },
            priority=7,
        )

    # ---- 权限/表单/重试 -------------------------------------------
    def enqueue_permission(self, att: dict, p: dict) -> None:
        task = self.ledger.get_task(att["task_id"])
        pid = p.get("id")
        self.enqueue_task(
            task, "blocked_permission",
            dedupe_key=f"permission:{att['id']}:{pid}",
            payload={
                "task_id": task["id"], "attempt_id": att["id"],
                "session_id": att["session_id"],
                "classification": "BLOCKED_PERMISSION",
                "permission_id": pid,
                "action": p.get("action"),
                "resources": p.get("resources"),
                "save": p.get("save"),
                "summary": f"权限等待：{p.get('action')} {p.get('resources')}",
                "next_action": (
                    "定位请求者并由有权限的人决定；不得自动批准；"
                    f"回复入口：oc-orch permission reply --session {att['session_id']} "
                    f"--request {pid} --decision reject|once|always"
                ),
                "needed_from": "human",
            },
            priority=10,
        )

    def handle_permission(self, att: dict, envelope: dict) -> None:
        data = envelope.get("data") or {}
        if att["status"] not in ("blocked_permission", "terminal_succeeded",
                                 "terminal_failed", "terminal_interrupted"):
            att = self.ledger.transition_attempt(
                att["id"], "blocked_permission", reason="permission-asked",
                actor="supervisor",
            )
        self.enqueue_permission(att, data)

    def handle_coordinator_permission(self, session_id: str, envelope: dict) -> None:
        data = envelope.get("data") or {}
        projects = self.projects_for_coordinator(session_id)
        if not projects:
            # 本 home 未跟踪的 session：不是我们的协调者，忽略
            return
        payload = {
            "session_id": session_id,
            "classification": "COORDINATOR_BLOCKED_PERMISSION",
            "permission_id": data.get("id"),
            "action": data.get("action"),
            "resources": data.get("resources"),
            "summary": "主协调 session 自身遇到权限等待",
            "next_action": "人工决定；不会自动批准",
        }
        for project in projects:
            self.ledger.enqueue_notification(
                kind="coordinator_blocked_permission", target_session=session_id,
                dedupe_key=f"coord-permission:{session_id}:{data.get('id')}",
                payload=payload, project=project, priority=10,
            )
        self.ledger.add_alert(
            severity="high", kind="coordinator_permission",
            title="主协调 session 权限等待",
            detail=json_dumps(payload, ensure_ascii=False),
            dedupe_key=f"coord-permission:{session_id}:{data.get('id')}",
        )

    def handle_permission_replied(self, att: dict, session_id: str | None) -> None:
        if not session_id:
            return
        try:
            pending = [p for p in self.client.permissions(att["directory"])
                       if p.get("sessionID") == session_id]
        except Exception:
            return
        if not pending and att["status"] == "blocked_permission":
            self.ledger.transition_attempt(att["id"], "running",
                                           reason="permission-resolved",
                                           actor="supervisor")
        # 已解决的权限通知不再唤醒
        rows = self.ledger.list_notifications(state="pending", limit=200)
        ids = [n["id"] for n in rows if n["kind"] == "blocked_permission"
               and n["attempt_id"] == att["id"]]
        if not pending and ids:
            self.ledger.ack_notifications(ids, actor="system:permission-resolved")

    def enqueue_form(self, att: dict, f: dict) -> None:
        task = self.ledger.get_task(att["task_id"])
        fid = f.get("id")
        self.enqueue_task(
            task, "blocked_form",
            dedupe_key=f"form:{att['id']}:{fid}",
            payload={
                "task_id": task["id"], "attempt_id": att["id"],
                "session_id": att["session_id"],
                "classification": "BLOCKED_FORM",
                "form_id": fid, "title": f.get("title"),
                "fields": [x.get("key") for x in (f.get("fields") or [])],
                "summary": f"表单等待：{f.get('title')}",
                "next_action": "由责任人或用户填写；不得自动冒充回答",
                "needed_from": "human",
            },
            priority=9,
        )

    def handle_form(self, att: dict, envelope: dict) -> None:
        data = envelope.get("data") or {}
        if att["status"] not in ("blocked_form", "terminal_succeeded",
                                 "terminal_failed", "terminal_interrupted"):
            att = self.ledger.transition_attempt(
                att["id"], "blocked_form", reason="form-created", actor="supervisor",
            )
        self.enqueue_form(att, data)

    def _attempt_model(self, att: dict) -> str | None:
        if att.get("model"):
            return str(att["model"])
        model = (att.get("session_meta") or {}).get("model") or {}
        if model.get("providerID") and model.get("id"):
            return f"{model['providerID']}/{model['id']}"
        return None

    def handle_retry(self, att: dict, envelope: dict, error: dict) -> None:
        cls = classify_provider_error(error)
        data = envelope.get("data") or {}
        meta = dict(att["metadata"])
        pr = dict(meta.get("provider_retry") or {})
        pr["count"] = int(pr.get("count") or 0) + 1
        pr["first_at"] = pr.get("first_at") or self.clock.now_ms()
        pr["last_at"] = self.clock.now_ms()
        pr["last_error"] = cls
        pr["attempt_no"] = data.get("attempt")
        meta["provider_retry"] = pr
        model = self._attempt_model(att)
        if model:
            if cls["transient"]:
                delay = min(900, 30 * (2 ** min(pr["count"], 5)))
            else:
                delay = 3600
            self.ledger.record_provider_state(
                model, category=cls["category"], status=cls["status"],
                until_ms=self.clock.now_ms() + delay * 1000,
                reason=cls["message"],
            )
        fields: dict[str, Any] = {"metadata": meta, "last_observed_at": self.clock.now_ms()}
        self.ledger.update_attempt(att["id"], fields=fields, actor="supervisor",
                                   reason="provider-retry")
        task = self.ledger.get_task(att["task_id"])
        if not cls["transient"]:
            if att["status"] not in ("provider_retry", "terminal_succeeded",
                                     "terminal_failed", "terminal_interrupted"):
                att = self.ledger.transition_attempt(
                    att["id"], "provider_retry",
                    reason=f"provider-{cls['category']}", actor="supervisor",
                )
            self.enqueue_task(
                task, "provider_blocked",
                dedupe_key=f"provider:{att['id']}:{cls['category']}:{cls['status']}",
                payload={
                    "task_id": task["id"], "attempt_id": att["id"],
                    "session_id": att["session_id"],
                    "classification": "PROVIDER_BLOCKED",
                    "category": cls["category"], "status": cls["status"],
                    "summary": f"provider {cls['category']}（{cls['status']}）："
                               f"{redact(cls['message'], 300)}",
                    "next_action": (
                        "认证/额度/模型不可用：明确阻塞；"
                        "替换模型或渠道须遵守当前授权，不无限自动重试"
                    ),
                    "needed_from": "human",
                },
                priority=9,
            )
            self.ledger.add_alert(
                severity="high", kind=f"provider_{cls['category']}",
                title=f"provider 阻塞：{task['id']}",
                detail=json_dumps({
                    "attempt": att["id"], "category": cls["category"],
                    "status": cls["status"], "message": redact(cls["message"], 500),
                }, ensure_ascii=False),
                dedupe_key=f"provider:{att['id']}:{cls['category']}:{cls['status']}",
                task_id=task["id"], attempt_id=att["id"],
            )
            return
        threshold = int(self.cfg["provider_retry_alert_threshold"])
        elapsed = self.clock.now_ms() - int(pr["first_at"])
        if pr["count"] >= threshold or elapsed >= int(self.cfg["provider_retry_alert_after"]) * 1000:
            if att["status"] not in ("provider_retry",):
                att = self.ledger.transition_attempt(
                    att["id"], "provider_retry", reason="provider-retry-threshold",
                    actor="supervisor",
                )
            bucket = pr["count"] // max(1, threshold)
            self.enqueue_task(
                task, "provider_retry",
                dedupe_key=f"provider-retry:{att['id']}:{bucket}",
                payload={
                    "task_id": task["id"], "attempt_id": att["id"],
                    "session_id": att["session_id"],
                    "classification": "PROVIDER_RETRY",
                    "category": cls["category"], "status": cls["status"],
                    "retry_count": pr["count"], "elapsed_ms": elapsed,
                    "summary": f"provider 暂时故障，已重试 {pr['count']} 次；保持原任务关联",
                    "next_action": "观察退避；不要通过扩 session 放大限流",
                },
                priority=5,
            )

    def enqueue_idle(self, att: dict, reason: str) -> None:
        task = self.ledger.get_task(att["task_id"])
        self.enqueue_task(
            task, "idle_unconfirmed",
            dedupe_key=f"idle:{att['id']}:{reason}",
            payload={
                "task_id": task["id"], "attempt_id": att["id"],
                "session_id": att["session_id"],
                "classification": "IDLE_UNCONFIRMED",
                "summary": f"session 不活跃且无终态（{reason}）；不能当作业务完成",
                "next_action": "检查 inbox/后台工具/内部委派；必要时发送继续指令",
            },
            priority=6,
        )

    # ---- 协调者观察 -----------------------------------------------
    def coordinator_for(self, task: dict) -> str | None:
        if task.get("coordinator_session"):
            return task["coordinator_session"]
        lease = self.ledger.coordinator_lease(task["project"])
        return lease["holder_session"] if lease else None

    def projects_for_coordinator(self, session_id: str) -> list[str]:
        projects = set()
        for lease in self.ledger.list_coordinator_leases(session_id=session_id):
            projects.add(lease["project"])
        for task in self.ledger.list_tasks(limit=500):
            if task.get("coordinator_session") == session_id and task["status"] not in (
                "accepted", "cancelled",
            ):
                projects.add(task["project"])
        return sorted(projects)

    def enqueue_task(self, task: dict, kind: str, *, dedupe_key: str,
                     payload: dict, priority: int = 5) -> dict | None:
        target = self.coordinator_for(task)
        if target:
            return self.ledger.enqueue_notification(
                kind=kind, target_session=target, dedupe_key=dedupe_key,
                payload=payload, project=task["project"], task_id=task["id"],
                attempt_id=payload.get("attempt_id"), priority=priority,
                max_attempts=int(self.cfg["max_wake_attempts"]),
            )
        self.ledger.add_alert(
            severity="high" if priority >= 7 else "warning",
            kind=kind,
            title=f"{kind}：{task['id']}",
            detail=json_dumps(payload, ensure_ascii=False),
            dedupe_key=dedupe_key,
            task_id=task["id"], attempt_id=payload.get("attempt_id"),
        )
        return None

    def _on_started(self, att: dict, envelope: dict) -> None:
        created = envelope.get("created") if isinstance(envelope.get("created"), int) else self.clock.now_ms()
        event_id = envelope.get("id") or f"started-{created}"
        durable = envelope.get("durable") if isinstance(envelope.get("durable"), dict) else {}
        out = self.ledger.record_execution_started(
            att["id"], started_at=created, event_id=event_id,
            seq=durable.get("seq"), source="sse", actor="supervisor")
        if out["result"] in ("accepted", "merged", "duplicate"):
            # 归属补齐后重试已保存但未确认的终态事件（T07：终态先于 started 到达）
            self._redrive_saved_terminal(self.ledger.get_attempt(att["id"]))

    def _on_coordinator_started(self, session_id: str | None) -> None:
        # execution.started 不是成功；不能在这里清零协调者失败计数，
        # 否则“失败→恢复→启动→失败”会无限循环。
        return

    def observe_coordinators(self, active: dict[str, dict]) -> None:
        sessions = set()
        for lease in self.ledger.list_coordinator_leases():
            sessions.add(lease["holder_session"])
        for task in self.ledger.list_tasks(limit=500):
            if task.get("coordinator_session") and task["status"] not in ("accepted", "cancelled"):
                sessions.add(task["coordinator_session"])
        for sid in sorted(sessions):
            if sid in active:
                # 活跃即视为租约心跳；不因“回合未结束”催办
                for project in self.projects_for_coordinator(sid):
                    lease = self.ledger.coordinator_lease(project)
                    if lease and lease["holder_session"] == sid:
                        try:
                            self.ledger.claim_coordinator(
                                project=project, session_id=sid, actor="supervisor",
                                ttl=int(self.cfg["coordinator_lease_ttl"]),
                            )
                        except LedgerError:
                            pass
                continue
            try:
                session = self.client.get_session(sid)
            except ApiError as e:
                if e.status == 404:
                    self.ledger.add_alert(
                        severity="high", kind="coordinator_lost",
                        title="主协调 session 不存在",
                        detail=f"{sid} 返回 404；需要人工接管 coordinator",
                        dedupe_key=f"coordinator-lost:{sid}",
                    )
                    for project in self.projects_for_coordinator(sid):
                        lease = self.ledger.coordinator_lease(project)
                        if lease:
                            self.ledger.block_coordinator_recovery(
                                project, reason="coordinator-session-404"
                            )
                continue
            if session is None:
                continue
            outcome = session.get("outcome")
            idle = int((session.get("time") or {}).get("idle") or 0)
            meta_key = f"coord_outcome:{sid}"
            seen = self.ledger.get_meta(meta_key)
            stamp = f"{outcome}:{idle}"
            if outcome == "succeeded":
                # 同一终态首轮补证可能失败；是否再次查证由持久观察预算决定，
                # 不能因 coord_outcome 已记 seen 永久跳过原 probe 的晚到成功。
                self._maybe_clear_coordinator_gate(sid, idle)
            if outcome in ("succeeded", "failed", "interrupted") and seen != stamp:
                self.ledger.set_meta(meta_key, stamp)
                if outcome == "succeeded":
                    # S3：只有可归属到本输入半开探测的新成功才清闸门；
                    # HTTP 受理/started/历史 succeeded 不构成清除证据。
                    for project in self.projects_for_coordinator(sid):
                        self.ledger.reset_coordinator_failure(project,
                                                              reason="coordinator-succeeded")
                else:
                    if outcome == "failed":
                        # REST 补证：只用当前 idle 消息的 error（匹配 time.idle/
                        # 本输入区间），不扫旧报错覆盖新成功。
                        self._observe_coordinator_rest_error(sid, idle)
                    self.handle_coordinator_terminal(
                        sid, outcome, None,
                        {"id": f"obs-coord-{sid}-{idle}", "created": idle,
                         "data": {"sessionID": sid}},
                    )
            if outcome in ("succeeded", "failed"):
                self._finish_registered_user_turn(sid, session)

    def handle_coordinator_terminal(self, session_id: str | None, outcome: str,
                                    reason: str | None, envelope: dict) -> None:
        if not session_id:
            return
        projects = self.projects_for_coordinator(session_id)
        if not projects:
            return
        event_id = envelope.get("id") or f"{session_id}:{outcome}"
        if event_id in self._coord_fail_handled:
            return
        self._coord_fail_handled.add(event_id)
        data = envelope.get("data") if isinstance(envelope.get("data"), dict) else {}
        interrupt_origin_at: int | None = None
        # 回合hold与项目暂停/provider gate独立；未登记输入不再自动清门。
        if outcome == "interrupted":
            try:
                created = envelope.get("created")
                known_origin = type(created) is int and created > 0
                interrupt_origin_at = (created if known_origin
                                       else self.clock.now_ms())
                hold_result = self.coordinator_control.hold_session(
                    session_id, event_id=event_id,
                    origin_at=interrupt_origin_at,
                    reason=(_COORD_TURN_INTERRUPT_REASON if known_origin
                            else "unattributed-interruption"))
                if hold_result.get("refused") == "stale-event":
                    return  # 迟到旧终态不再次延后当前回合的通知/失败计数。
            except Exception as exc:
                self._gate_state_error(session_id, f"turn-hold:{type(exc).__name__}")
        elif outcome == "failed" and isinstance(data.get("error"), dict):
            err_cls = classify_provider_error(data["error"])
            if err_cls.get("category") not in (None, "", "unknown"):
                created = envelope.get("created")
                at = created if isinstance(created, int) else self.clock.now_ms()
                self.target_gate.observe_error(
                    session_id, err_cls,
                    evidence_key=f"coord-terminal:{session_id}:{event_id}",
                    observed_at=self.clock.now_ms(),
                    origin_at=at,
                )
        for project in projects:
            lease = self.ledger.coordinator_lease(project)
            if lease and lease["holder_session"] != session_id:
                continue
            failure = (self.ledger.record_coordinator_failure(
                project, reason=outcome, key=event_id)
                if lease else {"failure_count": 1, "recovery_blocked": 0})
            # 协调者已失败：暂停向它重复投递旧通知，等待恢复/接管
            self.ledger.defer_notifications_for_session(
                session_id,
                next_attempt_at=self.clock.now_ms() + 3600 * 1000,
                reason=f"coordinator-{outcome}",
            )
            payload = {
                "project": project, "session_id": session_id,
                "classification": "COORDINATOR_FAILED" if outcome == "failed"
                                  else "COORDINATOR_INTERRUPTED",
                "outcome": outcome, "reason": reason,
                "failure_count": failure["failure_count"],
                "summary": f"主协调 session 执行 {outcome}；台账与待办保留",
                "next_action": (
                    "检查是否认证/额度问题；有限恢复只续接原 session，"
                    "不自动换模型；连续失败则人工接管"
                ),
            }
            self.ledger.add_alert(
                severity="high", kind=f"coordinator_{outcome}",
                title=f"主协调 {outcome}：{project}",
                detail=json_dumps(payload, ensure_ascii=False),
                dedupe_key=f"coord-failure:{project}:{event_id}",
            )
            if outcome == "interrupted":
                # 只由session hold阻止新自动输入；不永久封锁项目恢复，不清
                # 原有显式停用/人工暂停留下的lease状态。
                continue
            if (outcome == "failed" and lease and self.cfg["auto_coordinator_recovery"]
                    and not failure["recovery_blocked"]):
                self.ledger.enqueue_notification(
                    kind="coordinator_recovery", target_session=session_id,
                    dedupe_key=f"coord-recovery:{project}:{failure['failure_count']}",
                    payload={
                        "project": project, "session_id": session_id,
                        "classification": "COORDINATOR_RECOVERY",
                        "failure_count": failure["failure_count"],
                        "summary": "主协调上次执行失败；请从台账检查点续接，不要重做已验收工作",
                        "instructions": (
                            "先运行 oc-orch status --json 和 oc-orch inbox "
                            f"--session {session_id} --ack-all；"
                            "只恢复未完成责任；若为认证/额度/模型阻塞，保留告警，不要重试"
                        ),
                        "next_action": "读取台账，续接未完成通知；无需重发已完成派单",
                    },
                    project=project, priority=10, max_attempts=1,
                    next_attempt_at=self.clock.now_ms() + int(self.cfg["recovery_delay"]) * 1000,
                )
        if interrupt_origin_at is not None:
            # 中断时清掉“排队但未执行”的自有唤醒：queue 消息不会自行开启
            # 新回合，留在 inbox 会永久阻塞后续自动唤醒。
            # 只取消本系统登记且不晚于中断事实的消息；通知已由上面的
            # defer_notifications_for_session 降为 pending，释放 hold 后重发。
            self._cancel_own_queued_wakes(
                session_id, origin_at=interrupt_origin_at,
                reason="coordinator-interrupted")

    def _cancel_own_queued_wakes(self, session_id: str, *, origin_at: int,
                                 reason: str) -> list[str]:
        """中断后取消本系统排队唤醒；不认识的消息一律不动。

        - active 观察失败/目标仍活跃 → 不取消（可能已开始新回合）；
        - 只处理消息号属于本系统通知/输入/派单，且创建或发送时间不晚于
          中断事实的自有消息；
        - 通知状态不由这里 ACK；调用方已把它们 defer 为 pending；
        - 取消失败只记审计，留给空闲对账的 ``wake.recovered`` 路径。
        """
        try:
            if session_id in self.client.active_sessions():
                return []
        except Exception:
            return []
        try:
            inbox = list(self.client.inbox(session_id))
        except Exception:
            return []
        own = self._own_queued_input_ids(session_id)
        cancelled: list[str] = []
        for item in inbox:
            if not isinstance(item, dict):
                continue
            message_id = item.get("id")
            if not message_id or message_id not in own:
                continue
            notes = [n for n in self.ledger.list_notifications(
                target_session=session_id, limit=500)
                if n.get("message_id") == message_id]
            inp = self.ledger.input_for_receipt(session_id, message_id)
            # 只处理自动 wake：有输入槽时 source 必须是 automatic_wake；
            # 没有输入槽时至少有本系统通知身份。managed/record/attempt 不动。
            if inp is not None and inp.get("source") != "automatic_wake":
                continue
            if inp is None and not notes:
                continue
            created = (item.get("time") or {}).get("created")
            ref = created if isinstance(created, int) and not isinstance(created, bool) else None
            if ref is None:
                sent = [int(n.get("sent_at") or 0) for n in notes
                        if isinstance(n.get("sent_at"), int)]
                ref = max(sent) if sent else None
            if not isinstance(ref, int) or isinstance(ref, bool) or ref <= 0:
                continue  # 无法证明“不晚于中断”时不取消（fail-closed）。
            if ref > origin_at:
                continue  # 中断之后才到达的新输入不在本清理范围。
            try:
                self.client.cancel_inbox(session_id, message_id)
                # 主动取消是本地可信事实：立即释放未决槽位，不依赖可能丢失
                # 的 SSE cancelled 事件；通知状态仍由调用方维持 pending。
                try:
                    self.ledger.record_input_cancelled(
                        session_id=session_id, message_id=message_id,
                        reason=reason, now=self.clock.now_ms(),
                        actor="supervisor")
                except Exception as e:
                    self.ledger.audit(
                        "supervisor", "wake.cancel-reconcile-error", "session",
                        session_id, {"message_id": message_id, "reason": reason,
                                     "error": redact(repr(e), 200)})
            except Exception as e:
                self.ledger.audit(
                    "supervisor", "wake.cancel-error", "session", session_id,
                    {"message_id": message_id, "reason": reason,
                     "error": redact(repr(e), 200)})
                continue
            cancelled.append(message_id)
            self.ledger.audit(
                "supervisor", "wake.cancelled", "session", session_id,
                {"message_id": message_id, "reason": reason,
                 "notification_ids": [n["id"] for n in notes],
                 "input_id": inp.get("input_id") if inp else None})
        return cancelled

    def _finish_registered_user_turn(self, session_id: str, session: dict) -> bool:
        """与显式入口共享唯一的原ID完整窗口观察器；普通REST输入不取得控制权限。"""
        try:
            observer = make_registered_turn_observer(self.ledger, client=lambda: self.client,
                                                     clock=self.clock, window=self._window)
            return observer.observe(session_id, session)["finished"]
        except Exception as exc:
            self._gate_state_error(session_id, f"user-turn:{type(exc).__name__}")
            return False

    # ---- 通知投递 -------------------------------------------------
    def _fill_pending_routes(self) -> None:
        """route_pending 通知补路由（5.4）：coordinator 出现/接管后投递恢复。"""
        seen: set[str] = set()
        for lease in self.ledger.list_coordinator_leases():
            project, holder = lease["project"], lease["holder_session"]
            if project in seen:
                continue
            seen.add(project)
            try:
                self.ledger.fill_notification_routes(project, holder)
            except Exception as e:
                self.last_error = repr(e)
                self.ledger.audit("supervisor", "route.fill-error", "project",
                                  project, {"error": redact(repr(e), 300)})
        for task in self.ledger.list_tasks(limit=500):
            target = task.get("coordinator_session")
            if not target or task["project"] in seen:
                continue
            seen.add(task["project"])
            try:
                self.ledger.fill_notification_routes(task["project"], target)
            except Exception as e:
                self.last_error = repr(e)

    def dispatch_notifications(self) -> dict:
        now = self.clock.now_ms()
        # 有界回收成熟孤立 prepared 槽（崩溃/快速重启窗口）：不依赖启动时
        # recover_inflight 或新通知触发；从未 possible 的可安全释放重建，
        # 已 possible/unknown 严格保原 ID。
        try:
            self._reclaim_stale_prepared(now)
        except Exception:
            pass
        due = self.ledger.due_notifications(now=now,
                                            limit=int(self.cfg["notification_batch"]) * 3)
        summary = {"due": len(due), "sent": 0, "failed": 0, "suppressed": 0}
        if not due:
            return summary
        # active 快照失败 → 所有目标的活动性都是 unknown（P03）：失败不是
        # “空闲”，不能借空 active 集合继续追加自动输入。
        active_ok = True
        try:
            active_now = self.client.active_sessions()
        except Exception:
            active_now = {}
            active_ok = False
        groups: dict[str, list[dict]] = {}
        for n in due:
            groups.setdefault(n["target_session"], []).append(n)
        for target, items in groups.items():
            if len(items) > int(self.cfg["notification_batch"]):
                items = items[:int(self.cfg["notification_batch"])]
            # 暂停/取消任务的通知不发送
            sendable = []
            for n in items:
                try:
                    self.ledger.check_automatic_wake(target, [n])
                except LedgerError as exc:
                    self.ledger.defer_notification(
                        n["id"], next_attempt_at=now + self._wake_diagnostic_interval_ms(),
                        reason=f"control:{exc.code}")
                    if exc.code == "CONTROL_STATE_UNKNOWN":
                        self._gate_state_error(target, "coordinator-control:unknown")
                    summary["suppressed"] += 1
                    continue
                task = self.ledger.get_task(n["task_id"]) if n.get("task_id") else None
                if task and task["status"] == "paused":
                    self.ledger.defer_notification(
                        n["id"], next_attempt_at=now + 300 * 1000,
                        reason="task-paused",
                    )
                    summary["suppressed"] += 1
                    continue
                if task and task["status"] == "cancelled":
                    self.ledger.ack_notifications([n["id"]], actor="system:cancelled")
                    summary["suppressed"] += 1
                    continue
                if n["state"] == "pending" and n["attempts"] >= n["max_attempts"]:
                    self.ledger.mark_notification_dead(
                        n["id"], error="max-attempts-before-send", owner=self.instance
                    )
                    self.ledger.add_alert(
                        severity="high", kind="notification_dead",
                        title=f"通知未能送达：{n['kind']}",
                        detail=json_dumps(n, ensure_ascii=False, default=str),
                        dedupe_key=f"notif-dead:{n['id']}",
                        task_id=n.get("task_id"), attempt_id=n.get("attempt_id"),
                    )
                    summary["failed"] += 1
                    continue
                sendable.append(n)
            if not sendable:
                continue
            # 防止把新唤醒继续压入已有排队/未确认唤醒的 session。
            # active/inbox 观察采用 known/unknown：任一查询失败都不能当作
            # “空队列/空闲”而追加模型输入（P03）。
            inbox_known = True
            pending_inbox: list[dict] = []
            try:
                pending_inbox = self.client.inbox(target)
            except Exception:
                inbox_known = False
            outstanding = self.ledger.list_notifications(
                target_session=target, state="sent", limit=100
            )
            suppress, why = should_suppress_wake(
                outstanding=outstanding,
                inbox=pending_inbox,
                is_active=target in active_now,
                now_ms=now,
                ack_timeout_ms=int(self.cfg["ack_timeout"]) * 1000,
                known=bool(active_ok and inbox_known),
            )
            if suppress:
                recovered = self._recover_stale_own_wake(
                    target, inbox=pending_inbox,
                    active=target in active_now,
                    known=bool(active_ok and inbox_known), now=now)
                self._record_wake_suppression(
                    target, why, inbox=pending_inbox,
                    inbox_known=inbox_known, active_ok=active_ok,
                )
                for n in sendable:
                    if n["state"] == "pending":
                        self.ledger.defer_notification(
                            n["id"],
                            next_attempt_at=now + min(
                                300, int(self.cfg["ack_timeout"])
                            ) * 1000,
                            reason=f"wake-suppressed:{why}",
                        )
                summary["suppressed"] += len(sendable)
                self.ledger.audit(
                    "supervisor", "wake.suppressed", "session", target,
                    {"reason": why, "inbox_known": inbox_known,
                     "active_ok": active_ok, "inbox": len(pending_inbox),
                     "outstanding": len(outstanding), "count": len(sendable),
                     "recovered": recovered},
                )
                continue
            # S3：所有自动发送（新通知与 coordinator_recovery）发送前检查目标
            # 闸门；cooling/manual/exhausted/未决或未知半开不触发 prompt、不
            # 计通知预算（只 defer）。到期后在真实 POST 前原子 claim_probe。
            try:
                gate_snap = self.target_gate.get(target)
            except Exception as e:
                # E/G1：闸门读取/判定异常一律 fail-closed（未知=阻塞诊断），
                # 绝不 fallback 为“无闸门”放行自动发送。
                self._gate_state_error(
                    target, f"get:{type(e).__name__}:{redact(str(e), 200)}")
                for n in sendable:
                    if n["state"] == "pending":
                        self.ledger.defer_notification(
                            n["id"],
                            next_attempt_at=now + min(
                                300, int(self.cfg["ack_timeout"])) * 1000,
                            reason="wake-suppressed:gate-state-error",
                        )
                summary["suppressed"] += len(sendable)
                self.ledger.audit(
                    "supervisor", "wake.suppressed", "session", target,
                    {"reason": "target-gate:state-error",
                     "count": len(sendable)})
                continue
            if gate_snap and gate_snap["state"] in _GATE_PUSH_BLOCK_STATES:
                self._defer_for_gate_blocked(target, gate_snap, sendable, now)
                summary["suppressed"] += len(sendable)
                self.ledger.audit(
                    "supervisor", "wake.suppressed", "session", target,
                    {"reason": f"target-gate:{gate_snap['state']}",
                     "count": len(sendable)},
                )
                continue
            try:
                session = self.client.get_session(target)
            except ApiError as e:
                if e.status == 404:
                    for n in sendable:
                        self.ledger.mark_notification_dead(
                            n["id"], error="target-session-404", owner=self.instance
                        )
                    self.ledger.add_alert(
                        severity="high", kind="wake_target_lost",
                        title="主协调通道丢失",
                        detail=f"通知目标 session {target} 返回 404；需要人工接管",
                        dedupe_key=f"wake-target-lost:{target}",
                    )
                    summary["failed"] += len(sendable)
                else:
                    summary["failed"] += len(sendable)
                continue
            if session is None:
                continue
            # 历史回执：schema1 迁移为 unknown(outstanding=0) 的遗留 sent+message_id，
            # 先围绕原 (session,message) 对账；仍 unknown 则保留/告警+下一动作，不
            # 隐式 ACK、不猜历史成功，也绝不解锁新身份重播（正文未知/未证实 delivery）。
            if self._defer_legacy_unknown_receipt(target, sendable, now):
                summary["suppressed"] += len(sendable)
                self.ledger.audit(
                    "supervisor", "wake.suppressed", "session", target,
                    {"reason": "legacy-unknown-receipt", "count": len(sendable)})
                continue
            # S2：预留目标输入槽（候选 message_id 先于任何可能 POST 持久化；DB
            # 部分唯一索引保证同目标最多一条未决自动 wake）。
            reserve = self.ledger.reserve_target_input(
                target,
                notification_ids=[n["id"] for n in sendable],
                intent="execute", owner=self.instance,
                source="automatic_wake", now=now,
            )
            inp = reserve["input"]
            if inp["admission_state"] != ADMISSION_PREPARED or reserve.get("authority") is False:
                # 已有可能发出/未知/受理/投递的未决输入，或槽被他 owner/在飞持有：
                # 本批并入既有槽对账，绝不制造新身份或新 POST（DB 唯一索引 + CAS 仲裁）。
                outcome = self._reconcile_existing_input(target, inp, sendable, now)
                if outcome.get("freed"):
                    # 旧槽已消费（delivered + 全部 ACK）并退役：本轮按新槽重试该批。
                    reserve = self.ledger.reserve_target_input(
                        target,
                        notification_ids=[n["id"] for n in sendable],
                        intent="execute", owner=self.instance,
                        source="automatic_wake", now=now)
                    inp = reserve["input"]
                else:
                    summary[outcome["bucket"]] += outcome["count"]
                    continue
                if inp["admission_state"] != ADMISSION_PREPARED or reserve.get("authority") is False:
                    summary["suppressed"] += len(sendable)
                    self.ledger.audit(
                        "supervisor", "wake.suppressed", "session", target,
                        {"reason": f"input-slot-held:{inp['admission_state']}",
                         "count": len(sendable)})
                    continue
            input_id = inp["input_id"]
            outcome = self._dispatch_prepared_input(target, input_id, sendable, inp, now)
            summary[outcome["bucket"]] += outcome["count"]
        return summary

    # ---- 观察/队头抑制的责任化（P03/P04） --------------------------
    def _wake_diagnostic_interval_ms(self) -> int:
        """unknown/阻塞诊断的下一次对账间隔：有界，复用 ACK 期限。"""
        return max(1, int(self.cfg.get("ack_timeout") or 0)) * 1000

    def _get_wake_diagnostic(self, target: str) -> dict:
        raw = self.ledger.get_meta(f"wake-diagnostic:{target}")
        if not isinstance(raw, str):
            return {}
        rec = json_loads(raw, {})
        return rec if isinstance(rec, dict) else {}

    def _persist_wake_diagnostic(self, target: str, *, reason: str,
                                 interval_ms: int) -> dict:
        """持久记录首次/最后原因与 next_check；返回是否超过有界期限。

        首次抑制只排 next_check（不立刻告警）；此后每次抑制都推进 last，
        只有跨过 next_check 才允许生成去重告警——既保证责任可见，又不
        会每个 reconcile 都刷一条告警。
        """
        rec = self._get_wake_diagnostic(target)
        now = self.clock.now_ms()
        prior_check = int(rec.get("next_check") or 0)
        alert_due = bool(rec) and now >= prior_check
        rec.setdefault("first_at", now)
        if not rec.get("first_reason"):
            rec["first_reason"] = reason
        rec["last_at"] = now
        rec["last_reason"] = reason
        rec["count"] = int(rec.get("count") or 0) + 1
        rec["next_check"] = (now + interval_ms if alert_due
                             else prior_check or now + interval_ms)
        self.ledger.set_meta(f"wake-diagnostic:{target}", json_dumps(rec))
        return {"alert_due": alert_due, "record": rec}

    # ---- 空闲目标自有排队唤醒的受控恢复 --------------------------
    def _wake_recover_key(self, target: str, message_id: str) -> str:
        return f"wake-recover:{target}:{message_id}"

    def _wake_recover_record(self, target: str, message_id: str) -> dict:
        raw = self.ledger.get_meta(self._wake_recover_key(target, message_id))
        if not isinstance(raw, str):
            return {}
        rec = json_loads(raw, {})
        return rec if isinstance(rec, dict) else {}

    def _wake_recover_stuck_alert(self, target: str, message_id: str,
                                  rec: dict, now: int) -> None:
        """已尝试 steer 但消息仍在 inbox：超过 ACK 期限转可见责任。"""
        attempted = rec.get("attempted_at")
        if not isinstance(attempted, int) or isinstance(attempted, bool):
            return
        if now - attempted < int(self.cfg["ack_timeout"]) * 1000:
            return
        self.ledger.add_alert(
            severity="high", kind="wake_recover_stuck",
            title=f"自有排队唤醒恢复未生效：{target}",
            detail=(
                f"message_id={message_id} action={rec.get('action')} "
                f"attempted_at={iso(attempted)} count={rec.get('count')}；"
                "原消息仍在 inbox，目标仍空闲。owner=operator；"
                "next_action=核对服务端 steer 语义/会话状态后人工 flush 或 cancel，"
                "不得另造新 prompt 绕过队头"
            ),
            dedupe_key=f"wake-recover-stuck:{target}:{message_id}",
        )

    def _recover_stale_own_wake(self, target: str, *, inbox: list[dict],
                                active: bool, known: bool, now: int) -> dict:
        """空闲目标上自有排队唤醒的原 ID 有界恢复。

        背景：``delivery=queue`` 在回合执行中入队；用户中断后该消息不会自行
        开启新回合，而 ``should_suppress_wake`` 又会因 inbox 非空持续抑制，
        形成“旧唤醒不执行、新唤醒不发送”的死锁。

        安全边界：
        - 只在 active/inbox 观察已知且目标 inactive 时尝试；
        - 只处理队列恰好一条且消息号属于本系统登记身份的自有输入；
        - 有 live 通知（pending/sent）→ PATCH steer 原消息并恢复通知状态，
          不新建 prompt、不重复计 attempts；
        - 通知已终结（acked/dead）且属于 automatic_wake → 宽限后取消孤立消息；
          非 automatic_wake（managed/record 等）绝不自动取消；
        - 每条消息最多 ``max_wake_inbox_recover`` 次副作用，仍滞留则告警；
          陌生输入不进入本方法。
        """
        if not known:
            return {"changed": False, "reason": "observation-unknown"}
        if active:
            return {"changed": False, "reason": "target-active"}
        if len(inbox) != 1:
            return {"changed": False, "reason": f"queue-len:{len(inbox)}"}
        item = inbox[0] if isinstance(inbox[0], dict) else None
        message_id = item.get("id") if item else None
        if not message_id:
            return {"changed": False, "reason": "unknown-shape"}
        if message_id not in self._own_queued_input_ids(target):
            return {"changed": False, "reason": "foreign"}

        notes = [n for n in self.ledger.list_notifications(
            target_session=target, limit=500)
            if n.get("message_id") == message_id]
        live = [n for n in notes if n.get("state") in ("pending", "sent")]
        note = (sorted(live, key=lambda n: int(n.get("created_at") or 0))[-1]
                if live else None)

        # 宽限锚点：服务端 inbox 创建时间优先；缺失时退回通知发送/创建时间。
        created = (item.get("time") or {}).get("created")
        anchor = created if isinstance(created, int) and not isinstance(created, bool) else None
        if anchor is None and note is not None:
            anchor = note.get("sent_at") or note.get("created_at")
        if not isinstance(anchor, int) or isinstance(anchor, bool):
            return {"changed": False, "reason": "age-unknown"}
        if now - anchor < int(self.cfg["wake_inbox_recover_grace"]) * 1000:
            return {"changed": False, "reason": "within-grace"}

        inp = self.ledger.input_for_receipt(target, message_id)
        orphan = note is None
        if orphan:
            # 没有 live 通知时只允许回收本系统自动 wake 的孤立消息；
            # managed/record 等有独立责任人，绝不自动取消。
            if inp is None or inp.get("source") != "automatic_wake":
                return {"changed": False, "reason": "non-wake-input"}
            if now - anchor < int(self.cfg["ack_timeout"]) * 1000:
                return {"changed": False, "reason": "orphan-within-ack-timeout"}

        rec = self._wake_recover_record(target, message_id)
        count = int(rec.get("count") or 0)
        if count >= int(self.cfg["max_wake_inbox_recover"]):
            self._wake_recover_stuck_alert(target, message_id, rec, now)
            return {"changed": False, "reason": "recover-budget-exhausted"}
        last = rec.get("attempted_at")
        if isinstance(last, int) and not isinstance(last, bool) \
                and now - last < min(300, int(self.cfg["ack_timeout"])) * 1000:
            return {"changed": False, "reason": "recover-backoff"}

        action = "steer" if note is not None else "cancel-orphan"
        try:
            if note is not None:
                self.client.update_inbox_delivery(target, message_id,
                                                  delivery="steer")
                self.ledger.mark_notification_resumed(
                    note["id"], message_id=message_id,
                    next_attempt_at=now + int(self.cfg["ack_timeout"]) * 1000,
                    reason="idle-stale-own-wake", owner=self.instance)
            else:
                self.client.cancel_inbox(target, message_id)
                try:
                    self.ledger.record_input_cancelled(
                        session_id=target, message_id=message_id,
                        reason="idle-stale-orphan-wake", now=now,
                        actor="supervisor")
                except Exception as e:
                    self.ledger.audit(
                        "supervisor", "wake.cancel-reconcile-error", "session",
                        target, {"message_id": message_id,
                                 "error": redact(repr(e), 200)})
        except Exception as e:
            failed = {
                "count": count + 1, "attempted_at": now, "action": action,
                "notification_id": note["id"] if note else None,
                "result": "error", "error": redact(repr(e), 200),
            }
            self.ledger.set_meta(self._wake_recover_key(target, message_id),
                                 json_dumps(failed))
            self.ledger.audit(
                "supervisor", "wake.recover-error", "session", target,
                {"message_id": message_id, "action": action,
                 "notification_id": note["id"] if note else None,
                 "error": redact(repr(e), 300)})
            if failed["count"] >= int(self.cfg["max_wake_inbox_recover"]):
                self._wake_recover_stuck_alert(target, message_id, failed, now)
            return {"changed": False, "reason": f"error:{type(e).__name__}"}

        done = {
            "count": count + 1, "attempted_at": now, "action": action,
            "notification_id": note["id"] if note else None,
            "result": "ok", "input_id": inp.get("input_id") if inp else None,
        }
        self.ledger.set_meta(self._wake_recover_key(target, message_id),
                             json_dumps(done))
        self.ledger.audit(
            "supervisor", "wake.recovered", "session", target,
            {"message_id": message_id, "action": action,
             "notification_id": note["id"] if note else None,
             "input_id": inp.get("input_id") if inp else None,
             "queue_len": len(inbox), "age_ms": now - anchor,
             "reason": "idle-stale-own-wake"})
        return {"changed": True, "reason": action, "message_id": message_id,
                "notification_id": note["id"] if note else None}

    def _record_wake_suppression(self, target: str, why: str, *,
                                 inbox: list[dict], inbox_known: bool,
                                 active_ok: bool) -> None:
        """把观察/队头抑制转成有界的可见诊断责任，绝不自动处置陌生输入。

        - observation-unknown：active/inbox 查询失败 → 不等价空队列；延后
          并按预算补证，超过有界期限后去重告警（正常目标不受影响）。
        - inbox-pending：区分自有排队输入与陌生输入（P04）。陌生队头形成
          blocked_by_foreign_input 可见责任（owner/下一动作），系统不自动
          steer/delete 陌生输入；自有排队输入保持对账等待消费，不无限 defer。
        """
        interval_ms = self._wake_diagnostic_interval_ms()
        if why == "observation-unknown":
            diag = self._persist_wake_diagnostic(
                target, reason="observation-unknown", interval_ms=interval_ms)
            if diag["alert_due"]:
                rec = diag["record"]
                self._wake_block_alert(
                    target, kind="wake_observation_unknown",
                    detail=(
                        f"目标 {target} 的 active/inbox 观察持续未知；"
                        "未追加任何自动模型输入。"
                        f"first={iso(rec['first_at'])} "
                        f"last={iso(rec['last_at'])} count={rec['count']}"
                    ),
                    next_action=(f"{target} 观察仍未知：检查服务与网络连通；"
                                 "恢复前各通知保持待发不丢责任"),
                )
            return
        if why == "inbox-pending":
            own = self._own_queued_input_ids(target)
            foreign = [i for i in inbox
                       if self._inbox_item_foreign(i, own) is not None]
            if foreign:
                diag = self._persist_wake_diagnostic(
                    target, reason="blocked-by-foreign-input",
                    interval_ms=interval_ms)
                if diag["alert_due"]:
                    rec = diag["record"]
                    head = ",".join(str(i.get("id")) for i in foreign[:5])
                    self._wake_block_alert(
                        target, kind="foreign_input_blocked",
                        detail=(
                            f"目标 {target} 的非空队头阻塞新通知；"
                            f"foreign_head=[{head}] "
                            f"first={iso(rec['first_at'])} "
                            f"last={iso(rec['last_at'])} count={rec['count']}"
                        ),
                        next_action=("人工核对陌生队头（owner/来源未知）后决定 "
                                     "flush/steer/cancel；系统不自动处置陌生输入，"
                                     "也不绕过队头发新输入"),
                    )
            else:
                self._persist_wake_diagnostic(
                    target, reason="inbox-pending-self", interval_ms=interval_ms)
            return

    def _own_queued_input_ids(self, target: str) -> set[str]:
        """本系统曾为目标登记的输入身份：所有通知消息号、输入槽消息号、
        尝试派单消息号。

        通知被 `defer_notifications_for_session` 从 sent 降为 pending 后，
        消息号仍是自有身份；只按 state=sent 识别会把自有排队唤醒误判为
        foreign，进而永久阻塞。
        """
        ids: set[str] = set()
        for n in self.ledger.list_notifications(
                target_session=target, limit=500):
            if n.get("message_id"):
                ids.add(n["message_id"])
        for inp in self.ledger.list_pending_inputs(
                target_session=target, limit=500):
            if inp.get("message_id"):
                ids.add(inp["message_id"])
        for att in self.ledger.attempts_for_session(target, limit=50):
            if att.get("dispatch_msg_id"):
                ids.add(att["dispatch_msg_id"])
        return ids

    @staticmethod
    def _inbox_item_foreign(item: dict, own_ids: set[str]) -> str | None:
        """陌生输入判定：本系统从未登记该消息 ID 即 foreign（返回消息 ID）。"""
        iid = item.get("id")
        if iid and iid in own_ids:
            return None
        return iid or None

    def _wake_block_alert(self, target: str, *, kind: str, detail: str,
                          next_action: str) -> None:
        """去重阻塞告警：同一 target/kind 在未 ack 前只产生一份（新态唯一）。"""
        self.ledger.add_alert(
            severity="high", kind=kind,
            title=f"唤醒被阻塞：{target}",
            detail=(detail + f"；owner=coordinator 复核；next_action={next_action}"),
            dedupe_key=f"wake-block:{kind}:{target}",
        )

    # ---- S3 目标闸门：阻塞责任/告警/错误观察（fail-closed） -------------
    def _defer_for_gate_blocked(self, target, gate_snap, sendable, now):
        state = gate_snap.get("state") or "blocked"
        self._persist_wake_diagnostic(target, reason=f"target-gate:{state}",
                                      interval_ms=self._wake_diagnostic_interval_ms())
        for n in sendable:
            if n["state"] == "pending":
                self.ledger.defer_notification(
                    n["id"], next_attempt_at=now + min(300, int(self.cfg["ack_timeout"])) * 1000,
                    reason=f"wake-suppressed:gate:{state}",
                )
        self._maybe_gate_alert(target, state, gate_snap)

    def _maybe_gate_alert(self, target, state, gate_snap=None):
        """冷却中不打扰；exhausted/probe-unknown/manual 去重告警并保留下一动作。"""
        if state not in ("exhausted", "probe-unknown", "manual-blocked", "manual"):
            return
        snap = gate_snap or {}
        category = snap.get("category") or "-"
        if state in ("manual-blocked", "manual"):
            self.ledger.add_alert(
                severity="high", kind="target_gate_manual",
                title=f"目标 {target} 被人工中断",
                detail=(f"目标 {target} 处于人工 manual 闸门；系统不自动解锁，"
                        "不因冷却到期或新成功解除。next_action=人工接管/明确后续 "
                        "resume 入口；不要直接编辑 meta"),
                dedupe_key=f"wake-gate:manual:{target}",
            )
        elif state == "exhausted":
            self.ledger.add_alert(
                severity="high", kind="target_gate_exhausted",
                title=f"目标 {target} 自动唤醒探测预算耗尽（{category}）",
                detail=(f"协调目标 {target} 的单路半开探测已用尽 "
                        f"(probe_count={snap.get('probe_count')})；待发通知保持 "
                        "pending 不丢责任。next_action=人工核对 provider/额度恢复后"
                        "接管或显式解除；不无限自动探测"),
                dedupe_key=f"wake-gate:exhausted:{target}",
            )
        else:
            observation = snap.get("probe_observation") or {}
            self.ledger.add_alert(
                severity="high", kind="target_gate_probe_unknown",
                title=f"目标 {target} 半开探测结果未知",
                detail=(f"协调目标 {target} 的半开探测超过期限仍无法归因；"
                         f"probe_message={snap.get('probe_message_id')}；"
                         f"owner=operator；补证次数={observation.get('checks', 0)}；"
                         f"补证期限={observation.get('deadline_at')}；"
                        "保留原 message 身份不换 ID。next_action=人工核对 inbox/执行"
                        "证据后决定继续或接管"),
                dedupe_key=f"wake-gate:probe-unknown:{target}",
            )

    def _gate_state_error(self, target, detail):
        """闸门状态损坏/读取异常：fail-closed 诊断；绝不当作无闸门放行。"""
        self.ledger.add_alert(
            severity="high", kind="target_gate_state_error",
            title=f"目标 {target} 闸门状态损坏/未知",
            detail=(f"{detail}；自动发送已 fail-closed 延后，保留原输入与身份。"
                    "next_action=人工检查 target-gate 记录与 schema，接管并明确后续 resume"),
            dedupe_key=f"wake-gate-state-error:{target}",
        )

    def _observe_coordinator_gate_error(self, session_id, error, *,
                                        evidence_key, observed_at=None,
                                        origin_at=None):
        """SSE 结构化 provider 错误 → 目标闸门。

        origin_at=稳定错误执行/事件源时间（D round3 水位线事实判定源）；
        observed_at=本地本次观测。未决半开期间，非 probe-fail 键只作为安全
        守卫（probe-unattributed），保留原 ID；绝不伪造 probe-fail:。
        """
        cls = classify_provider_error(error)
        if cls.get("category") in (None, "", "unknown"):
            return
        at = observed_at if isinstance(observed_at, int) else self.clock.now_ms()
        try:
            self.target_gate.observe_error(
                session_id, cls, evidence_key=evidence_key,
                observed_at=at, origin_at=origin_at, source="provider")
        except Exception as exc:
            self._gate_state_error(session_id,
                                   f"observe_error:{type(exc).__name__}:{redact(str(exc), 200)}")

    def _current_idle_error(self, session_id, idle_t):
        """读取最近消息，只取 type=idle 且 created==idle_t 的那条的 error。"""
        try:
            msgs = self.client.messages(session_id, limit=50, order="desc")
        except Exception:
            return None
        for m in msgs:
            if m.get("type") != "idle":
                continue
            if _message_created(m) != idle_t:
                continue
            err = m.get("error")
            return err if isinstance(err, dict) else None
        return None

    def _coordinator_probe_interval(self, session_id, probe_msg, started, idle_t):
        """完整窗口内只有原输入和唯一 idle；额外输入/多重终态均不猜归属。"""
        try:
            messages, coverage = self._window(session_id, since_ms=started,
                                              until_ms=idle_t)
        except Exception:
            return None
        if not coverage.get("complete"):
            return None
        probes = [m for m in messages if m.get("type") == "user"
                  and m.get("id") == probe_msg
                  and started <= _message_created(m) <= idle_t]
        if len(probes) != 1:
            return None
        probe_at = _message_created(probes[0])
        interval = [m for m in messages if probe_at <= _message_created(m) <= idle_t]
        users = [m for m in interval if m.get("type") == "user"]
        idles = [m for m in interval if m.get("type") == "idle"]
        if len(users) != 1 or len(idles) != 1:
            return None
        if any(m.get("type") in ("synthetic", "compaction", "shell", "skill")
               for m in interval):
            return None
        idle = idles[0]
        if (not idle.get("id") or _message_created(idle) != idle_t
                or idle_t <= probe_at):
            return None
        return idle

    def _maybe_clear_coordinator_gate(self, session_id, idle_t):
        """对原 outstanding/unknown probe 有界补证；完整唯一成功才以 CAS 清门。

        相同 session 终态的补证可重试，但不重投输入；HTTP 受理/started/
        历史 succeeded 不构成清除证据，网络期间的控制变化必须重新核验。
        """
        if (not isinstance(idle_t, int) or isinstance(idle_t, bool)
                or idle_t <= 0):
            return
        try:
            snap = self.target_gate.get(session_id)
        except Exception as exc:
            self._gate_state_error(session_id, f"clear.get:{type(exc).__name__}")
            return
        if not snap or snap.get("state") not in ("probe-outstanding", "probe-unknown"):
            return
        probe_msg = snap.get("probe_message_id")
        started = snap.get("probe_started_at")
        if not (isinstance(probe_msg, str) and probe_msg) or not isinstance(started, int):
            return
        if idle_t < started:
            return
        try:
            claim = self.target_gate.claim_probe_observation(
                session_id, message_id=probe_msg, expected_revision=snap["revision"],
                idle_at=idle_t)
        except Exception as exc:
            self._gate_state_error(session_id,
                                   f"probe-observation:{type(exc).__name__}")
            return
        if not claim.get("allowed"):
            if claim.get("reason") == "observation-exhausted":
                self._maybe_gate_alert(session_id, "probe-unknown", claim.get("gate"))
            return
        snap = claim["gate"]
        idle = self._coordinator_probe_interval(session_id, probe_msg, started, idle_t)
        if idle is not None and idle.get("outcome") == "succeeded":
            try:
                self.target_gate.record_success(
                    session_id, message_id=probe_msg, observed_at=idle_t,
                    evidence_key=f"coord-success:{session_id}:{idle['id']}",
                    expected_revision=snap["revision"])
            except Exception as exc:
                self._gate_state_error(session_id,
                                       f"record_success:{type(exc).__name__}")

    def _observe_coordinator_rest_error(self, session_id, idle_t):
        """REST 补证：只用当前 idle（created==session.time.idle）附带的 error，
        observed_at 用该 idle 的源时间；不扫描旧报错覆盖新成功。

        - 证明确为当前 probe 执行失败 → canonical `probe-fail:{原ID}`（结束该
          半开进入下一冷却）；把无归属错误伪装成 probe-fail 是禁止的；
        - 否则用稳定 rest-idle 键（未决半开期间得到 probe-unattributed 守卫）。
        """
        if not isinstance(idle_t, int) or idle_t <= 0:
            return
        err = self._current_idle_error(session_id, idle_t)
        if not isinstance(err, dict) or not err:
            return
        cls = classify_provider_error(err)
        if cls.get("category") in (None, "", "unknown"):
            return
        ev_key = f"rest-idle:{session_id}:{idle_t}"
        try:
            snap = self.target_gate.get(session_id)
        except Exception as exc:
            self._gate_state_error(session_id, f"rest.get:{type(exc).__name__}")
            return
        if snap and snap.get("state") == "probe-outstanding":
            probe_msg = snap.get("probe_message_id")
            started = snap.get("probe_started_at")
            if (isinstance(probe_msg, str) and probe_msg and isinstance(started, int)
                    and idle_t >= started):
                idle = self._coordinator_probe_interval(
                    session_id, probe_msg, started, idle_t)
                if idle is not None and idle.get("outcome") in ("failed", "interrupted"):
                    ev_key = f"{PROBE_FAILURE_KEY_PREFIX}{probe_msg}"
        try:
            self.target_gate.observe_error(
                session_id, cls, evidence_key=ev_key,
                observed_at=self.clock.now_ms(),
                origin_at=idle_t, source="provider")
        except Exception as exc:
            self._gate_state_error(session_id,
                                   f"rest.observe:{type(exc).__name__}")

    # ---- S2 输入回执：发送门 / 唯一赢家 / 错误处理 / 对账退役 ---------
    def _defer_claimed(self, claimed, target, now, *, reason):
        interval = min(300, int(self.cfg["ack_timeout"])) * 1000
        for n in claimed:
            if n["state"] in ("pending", "inflight"):
                self.ledger.defer_notification(n["id"], next_attempt_at=now + interval,
                                               reason=f"wake-suppressed:{reason}")

    def _dead_members_alert(self, claimed):
        for n in claimed:
            try:
                row = self.ledger.get_notification(n["id"])
            except Exception:
                continue
            if row["state"] == "dead":
                self.ledger.add_alert(
                    severity="high", kind="notification_dead",
                    title=f"通知重试耗尽：{row['kind']}",
                    detail=json_dumps(row, ensure_ascii=False, default=str),
                    dedupe_key=f"notif-dead:{row['id']}",
                    task_id=row.get("task_id"), attempt_id=row.get("attempt_id"),
                )

    def _dispatch_prepared_input(self, target, input_id, sendable, inp, now):
        """prepared 槽的发送准备：P06 claim 复核 → finalize 冻结（owner/CAS）→ 发送门。"""
        claim = self.ledger.claim_notifications([n["id"] for n in sendable],
                                                self.instance)
        if not claim:
            self.ledger.release_input_slot(input_id, reason="empty-claim", now=now)
            return {"bucket": "suppressed", "count": 0}
        try:
            fnd = self.ledger.finalize_input(
                input_id,
                final_notification_ids=[n["id"] for n in claim],
                text_factory=lambda rows: self.compose_wake(
                    target, self._rows_with_payload(rows)),
                owner=self.instance,
                expected_revision=int(inp.get("revision") or 1),
                now=now)
        except LedgerError as e:
            code = getattr(e, "code", "?")
            if code in ("INPUT_REVISION_CONFLICT", "INPUT_OWNER_MISMATCH"):
                # 另一发送者已最终化/占用：保持槽位与成员，退回待办（不释放）。
                self._defer_claimed(claim, target, now, reason="finalize-cas-loss")
                return {"bucket": "suppressed", "count": len(claim)}
            self.ledger.release_input_slot(
                input_id, reason=f"finalize:{code}", now=now)
            self._defer_claimed(claim, target, now, reason="finalize-failed")
            return {"bucket": "suppressed", "count": len(claim)}
        fsnap = fnd["input"]
        return self._send_wake_input(
            target, input_id=input_id,
            message_id=fsnap.get("message_id") or inp["message_id"],
            body=fnd["text"],
            notification_ids=fsnap.get("notification_ids") or [],
            revision=int(fsnap.get("revision") or 1),
            claimed=claim, now=now)

    def _send_wake_input(self, target, *, input_id, message_id, body,
                         notification_ids, claimed, now, revision=1):
        """真实 POST 前的发送门：S3 半开原子占用 + S2 唯一赢家标记。

        - claim_probe 未获准/闸门异常 → 退回待办不计预算（fail-closed）；
        - mark_input_possible 只有 changed 的唯一赢家才可 POST（DB/CAS 仲裁）；
        - HTTP 受理只代表已受理/排队，不代表模型成功（不在此 record_success）。
        返回 {"bucket": "sent"|"failed"|"suppressed", "count": n}。
        """
        interval = min(300, int(self.cfg["ack_timeout"])) * 1000
        # 远端观察期间可能刚提交暂停；先复核控制再占provider探测。
        # mark_input_possible还会在同一短事务中复核冻结成员及控制。
        try:
            self.ledger.check_automatic_wake(target, claimed)
        except LedgerError as exc:
            self._defer_claimed(claimed, target, now, reason=f"control:{exc.code}")
            return {"bucket": "suppressed", "count": len(claimed)}
        try:
            from ..application.delivery_action import model_observation

            observed_model = (model_observation(self.ledger, self.client, target)
                              if self.ledger.delivery_input_required(input_id) else None)
            admission = make_wake_admission(self.ledger, self.target_gate).claim(
                target, input_id=input_id, message_id=message_id, owner=self.instance,
                expected_revision=revision, now=now, model_observation=observed_model)
        except Exception as exc:
            self._gate_state_error(target, f"wake-admission:{type(exc).__name__}")
            self._defer_claimed(claimed, target, now, reason="gate-state-error")
            try:
                self.ledger.defer_input(input_id, next_reconcile_at=now + interval,
                                        reason="gate-state-error", now=now)
            except Exception:
                pass
            self.ledger.audit("supervisor", "wake.suppressed", "session", target,
                              {"reason": "target-gate:state-error",
                               "count": len(claimed)})
            return {"bucket": "suppressed", "count": len(claimed)}
        if not admission["allowed"]:
            probe = admission.get("probe")
            reason = admission["reason"]
            self._defer_claimed(claimed, target, now,
                                reason=reason)
            try:
                self.ledger.defer_input(input_id, next_reconcile_at=now + interval,
                                        reason=reason, now=now)
            except Exception:
                pass
            self.ledger.audit("supervisor", "wake.suppressed", "session", target,
                              {"reason": f"wake-admission:{reason}",
                               "count": len(claimed)})
            if probe:
                deny_state = ((probe.get("state") or {}).get("state") or probe["reason"])
                self._maybe_gate_alert(target, deny_state, probe.get("state"))
            elif reason == "control:CONTROL_STATE_UNKNOWN":
                self._gate_state_error(target, "wake-admission:control-state-unknown")
            return {"bucket": "suppressed", "count": len(claimed)}
        for n in claimed:
            self.ledger.set_notification_message(n["id"], message_id=message_id)
        from ..domain.delivery_action import check_handoff

        try:
            check_handoff(admission.get("permit"), self.clock.now_ms())
        except LedgerError as exc:
            self._record_input_unknown(input_id, exc.code, now, backoff_ms=interval)
            self._defer_claimed(claimed, target, now, reason=exc.code)
            return {"bucket": "suppressed", "count": len(claimed)}
        try:
            self.client.prompt(
                target, message_id=message_id, text=body, delivery="queue",
                metadata={"oc_orch_notifications": notification_ids,
                          "oc_orch_wake": True},
            )
        except ApiError as e:
            return self._handle_prompt_api_error(
                target, input_id, message_id, claimed, now, e)
        except TransportError as e:
            return self._handle_prompt_transport_error(
                target, input_id, message_id, claimed, now, e)
        else:
            # HTTP 受理：通知标记 sent（受理证据），输入保持 possible，等待
            # enqueued/delivered 事件补证；不当作模型成功清除任何闸门。
            for n in claimed:
                if n["state"] in ("pending", "inflight"):
                    self.ledger.mark_notification_sent(
                        n["id"], message_id=message_id,
                        next_attempt_at=now + int(self.cfg["ack_timeout"]) * 1000,
                        owner=self.instance,
                    )
            return {"bucket": "sent", "count": len(claimed)}

    def _record_input_unknown(self, input_id, reason, now, *, backoff_ms):
        """响应未知：保留原 message_id 与槽位；REST 对账后才能以原 ID 有界重试。"""
        deadline = now + int(self.cfg["ack_timeout"]) * 1000
        try:
            self.ledger.record_input_unknown(
                input_id, reason=reason, next_reconcile_at=now + backoff_ms,
                reconcile_deadline=deadline, now=now)
        except Exception:
            pass

    def _handle_prompt_api_error(self, target, input_id, message_id, claimed,
                                 now, e):
        cls = classify_http_error(e.status, e.tag, e.message)
        if e.status == 404:
            for n in claimed:
                if n["state"] in ("pending", "inflight"):
                    self.ledger.mark_notification_dead(
                        n["id"], error=f"prompt-404:{e}", owner=self.instance)
            self.ledger.add_alert(
                severity="high", kind="wake_target_lost",
                title="主协调通道丢失",
                detail=f"prompt 到 {target} 返回 404",
                dedupe_key=f"wake-target-lost:{target}",
            )
            try:
                self.ledger.record_input_cancelled(input_id=input_id,
                                                   reason="target-404", now=now)
            except Exception:
                pass
            self._dead_members_alert(claimed)
            return {"bucket": "failed", "count": len(claimed)}
        if e.status in (401, 403):
            for n in claimed:
                if n["state"] in ("pending", "inflight"):
                    self.ledger.mark_notification_retry(
                        n["id"], error=f"auth:{e}",
                        next_attempt_at=now + 3600 * 1000,
                        owner=self.instance, count=True)
            self.ledger.add_alert(
                severity="high", kind="wake_auth",
                title="唤醒通道认证失败",
                detail=redact(f"{e}", 500),
                dedupe_key=f"wake-auth:{self.client.service.base}",
            )
            self._record_input_unknown(input_id, f"auth:{e}", now,
                                       backoff_ms=3600 * 1000)
            self._dead_members_alert(claimed)
            return {"bucket": "failed", "count": len(claimed)}
        if e.status == 409:
            # 冲突：用统一四态分类消费受理证据；只有正向 admitted/delivered 才确认，
            # unknown（查询失败/坏形状）保持未知回执，不当作明确未受理继续盲重投。
            admit_ev = self._check_input_evidence(target, message_id)
            if admit_ev in ("admitted", "delivered"):
                try:
                    self.ledger.confirm_input_admitted(
                        input_id, evidence={"via": "409-inbox"}, now=now)
                except Exception:
                    pass
                self._dead_members_alert(claimed)
                return {"bucket": "sent", "count": len(claimed)}
            for n in claimed:
                if n["state"] in ("pending", "inflight"):
                    self.ledger.mark_notification_retry(
                        n["id"], error=f"conflict:{e}",
                        next_attempt_at=now + 60 * 1000,
                        owner=self.instance, count=True)
            self._record_input_unknown(input_id, f"conflict:{e}", now,
                                       backoff_ms=60 * 1000)
            self._dead_members_alert(claimed)
            return {"bucket": "failed", "count": len(claimed)}
        backoff = self._notify_backoff(claimed)
        for n in claimed:
            if n["state"] in ("pending", "inflight"):
                self.ledger.mark_notification_retry(
                    n["id"], error=f"{cls['category']}:{e}",
                    next_attempt_at=now + backoff * 1000,
                    owner=self.instance, count=bool(cls["transient"]))
        self._record_input_unknown(input_id, f"{cls['category']}:{e}", now,
                                   backoff_ms=backoff * 1000)
        self._dead_members_alert(claimed)
        return {"bucket": "failed", "count": len(claimed)}

    def _handle_prompt_transport_error(self, target, input_id, message_id,
                                       claimed, now, e):
        backoff = self._notify_backoff(claimed)
        for n in claimed:
            if n["state"] in ("pending", "inflight"):
                self.ledger.mark_notification_retry(
                    n["id"], error=f"transport:{e}",
                    next_attempt_at=now + backoff * 1000,
                    owner=self.instance, count=False)
        # P07/S2：响应未知 → 保留原 message_id 与槽位；REST 对账后才以原 ID 重试。
        self._record_input_unknown(input_id, f"transport:{e}", now,
                                   backoff_ms=backoff * 1000)
        self._dead_members_alert(claimed)
        return {"bucket": "failed", "count": len(claimed)}

    def _check_input_evidence(self, session_id, message_id):
        """REST 对账原 (session, msg) 四类分类，调用方必须按此消费：

        - admitted / delivered：正向受理/投递证据；
        - absent：两路查询都成功且确证原 ID 不存在 → 可证明未受理（唯一允许
          原 ID 有界重试的证据）；
        - unknown：任一查询失败/协议异常/坏形状/冲突，或入参缺失 → 未知，
          只延后+期限告警，绝不 POST、绝不换身份。
        不在 except 时返回 None 让调用方误作“缺失”。"""
        if not session_id or not message_id:
            return "unknown"
        inbox_ok = True
        try:
            if any(i.get("id") == message_id
                   for i in self.client.inbox(session_id)):
                return "admitted"
        except Exception:
            inbox_ok = False
        try:
            if self.client.get_message(session_id, message_id) is not None:
                return "delivered"
        except Exception:
            return "unknown"
        if not inbox_ok:
            return "unknown"  # inbox 查询失败：不能证明“确证未受理”
        return "absent"

    def _input_overdue_alert(self, inp, now):
        deadline = inp.get("reconcile_deadline")
        if not isinstance(deadline, int) or now < deadline:
            return
        self.ledger.add_alert(
            severity="high", kind="input_reconcile_overdue",
            title=f"目标 {inp['session_id']} 输入对账超期",
            detail=(f"输入 {inp['input_id']}（message={inp['message_id']}）超过对账"
                    f"期限（{deadline}）仍为 {inp['admission_state']}；保持原 ID 不换身份。"
                    "next_action=人工核对 inbox/执行证据后决定继续或接管"),
            dedupe_key=f"input-overdue:{inp['input_id']}",
        )

    def _consumed_delivered_input(self, inp, now):
        """已投递且全部业务成员已 ACK 才视为已消费槽；unknown/pending 不可退役。"""
        if inp.get("admission_state") != ADMISSION_DELIVERED:
            return False
        ids = inp.get("notification_ids") or []
        if not ids:
            return False
        for nid in ids:
            try:
                row = self.ledger.get_notification(nid)
            except Exception:
                return False
            if row["state"] in ("pending", "sent", "inflight"):
                return False
        return True

    def _retire_consumed_input(self, inp, now):
        """已消费 delivered+acked 旧槽：经 release_input_slot 安全退役（B：
        delivered/cancelled 且全部批次成员已终结 → delivered-retire，保留回执），
        避免永占目标；仅 ACK/空 inbox 不足够退役 unknown/possible。"""
        try:
            out = self.ledger.release_input_slot(
                inp["input_id"], reason="delivered-and-acked", now=now,
                actor="supervisor")
            return bool(out.get("released"))
        except Exception as exc:
            self.last_error = repr(exc)
            return False

    def _defer_input_wait(self, target, inp, sendable, now, *, reason):
        interval = min(300, int(self.cfg["ack_timeout"])) * 1000
        try:
            self.ledger.defer_input(inp["input_id"], next_reconcile_at=now + interval,
                                    reason=reason, now=now)
        except Exception:
            pass
        for n in sendable:
            if n["state"] == "pending":
                self.ledger.defer_notification(n["id"], next_attempt_at=now + interval,
                                               reason=f"wake-suppressed:{reason}")
        self._input_overdue_alert(inp, now)
        return {"bucket": "suppressed", "count": len(sendable)}

    def _nudge_delivered_input(self, target, inp, sendable, now) -> bool:
        """仅确证 delivered + 业务未 ACK 的有限催办；跨代总发送 ≤ max_wake_attempts。

        B.nudge_input 原子替换（prior/replaced_by 历史保留，原通知身份不变）；
        nudge 只在目标 inactive、inbox 空、非冷却/观察未知时触发（dispatch 前序
        抑制与闸门已把守）；等待/冷却不消耗催办预算。发送门仍走 S3 半开 + S2
        唯一赢家（owner/revision CAS）。返回是否已发起新的物理输入。
        """
        if inp.get("admission_state") != ADMISSION_DELIVERED:
            return False
        ids = inp.get("notification_ids") or []
        if not ids:
            return False
        cap = max(0, int(self.cfg.get("max_wake_attempts") or 0) - 1)
        if cap <= 0:
            return False
        try:
            out = self.ledger.nudge_input(
                inp["input_id"],
                text_factory=lambda rows: self.compose_wake(
                    target, self._rows_with_payload(rows)),
                owner=self.instance, now=now,
                metadata={"oc_orch_wake": True},
                max_nudges=cap)
        except LedgerError as e:
            if getattr(e, "code", "") == "NUDGE_BUDGET_EXHAUSTED":
                self._nudge_budget_alert(target, inp, now)
            return False
        except Exception:
            return False
        if not out.get("created") or not out.get("input"):
            return False  # 全部已 ACK / 后继已存在：退回不烧预算
        ninput = out["input"]
        result = self._send_wake_input(
            target, input_id=ninput["input_id"],
            message_id=ninput.get("message_id") or "",
            body=ninput.get("body") or "",
            notification_ids=ninput.get("notification_ids") or [],
            revision=int(ninput.get("revision") or 1),
            claimed=sendable, now=now)
        return result["bucket"] == "sent"

    def _nudge_budget_alert(self, target, inp, now):
        """跨代催办预算耗尽：去重可见责任，保留原通知身份。"""
        self.ledger.add_alert(
            severity="warning", kind="nudge_budget_exhausted",
            title=f"目标 {target} 催办预算耗尽",
            detail=(f"输入 {inp['input_id']}（msg={inp['message_id']}）已 delivered 但业务"
                    f"仍未 ACK；跨代发送已达 max_wake_attempts="
                    f"{self.cfg.get('max_wake_attempts')} 上限，等待不消费预算。"
                    "next_action=人工确认业务反馈或接管"),
            dedupe_key=f"nudge-budget-exhausted:{target}",
        )

    def _defer_legacy_unknown_receipt(self, target, sendable, now) -> bool:
        """遗留 unknown 回执（schema1 迁移：outstanding=0）成员：对账原
        (session,message)，不新建身份重播；仍 unknown → 保留/告警+下一动作。"""
        handled = False
        for n in sendable:
            try:
                receipts = self.ledger.notification_inputs(n["id"], limit=5)
            except Exception:
                continue
            for r in receipts:
                if (r.get("admission_state") != ADMISSION_UNKNOWN
                        or r.get("outstanding")):
                    continue
                sid = r.get("session_id") or target
                msg = r.get("message_id")
                ev = self._check_input_evidence(sid, msg) if msg else None
                if ev in ("admitted", "delivered"):
                    try:
                        if ev == "admitted":
                            self.ledger.confirm_input_admitted(
                                r["input_id"], evidence={"via": "legacy-reconcile"}, now=now)
                        else:
                            if r["admission_state"] in (ADMISSION_PREPARED,
                                                        ADMISSION_POSSIBLE,
                                                        ADMISSION_UNKNOWN):
                                self.ledger.confirm_input_admitted(
                                    r["input_id"], evidence={"via": "legacy-reconcile-delivered"},
                                    now=now)
                            self.ledger.confirm_input_delivered(
                                r["input_id"], evidence={"via": "legacy-reconcile"}, now=now)
                    except Exception:
                        pass
                    handled = True
                    continue
                # 仍 unknown：保留 + 去重可见告警 + 下一动作；不隐式 ACK。
                self.ledger.add_alert(
                    severity="warning", kind="legacy_unknown_receipt",
                    title=f"遗留输入回执仍未知：{sid}",
                    detail=(f"n={n['id']} msg={msg}；原 (session,message) 未证实 delivery 且"
                            "正文未知，不解锁新身份重播、不隐式 ACK、不猜历史成功。"
                            "next_action=人工核对原消息与执行证据后决定接管/补证"),
                    dedupe_key=f"legacy-unknown-receipt:{r['input_id']}",
                )
                handled = True
        return handled

    def _reclaim_stale_prepared(self, now) -> int:
        """有界回收成熟孤立 prepared 槽（崩溃/快速重启窗口，不依赖启动 recover）。

        - 仅 owner 已变更、从未 possible（从未 POST）且超过对账期限的 prepared：
          经 release_input_slot 安全释放（保留历史回执），并把其仍 inflight 的
          批次成员归还 pending（有界、仅本输入成员），使原责任可在正常 dispatch
          恢复（总计只 POST 一次）；
        - 已 possible/unknown/delivered/admitted 一律不释放、不换身份；
        - 未过期或同 owner 的 prepared 由正常发送/并发路径处理，不干扰活跃
          网络窗口的两个写者。返回回收槽数量。
        """
        reclaimed = 0
        for inp in self.ledger.list_pending_inputs(outstanding=True, limit=200):
            if inp.get("admission_state") != ADMISSION_PREPARED:
                continue
            if inp.get("claim_owner") == self.instance:
                continue  # 自己的 prepared 槽由准备发送路径处理
            deadline = inp.get("reconcile_deadline")
            if not isinstance(deadline, int):
                deadline = int(inp.get("prepared_at") or inp.get("created_at") or 0)                     + int(self.cfg["ack_timeout"]) * 1000
            if now < deadline:
                continue
            try:
                rel = self.ledger.release_input_slot(
                    inp["input_id"], reason="stale-prepared-never-possible",
                    now=now, actor="supervisor")
            except Exception:
                continue
            if not rel.get("released"):
                continue
            for nid in inp.get("notification_ids") or []:
                try:
                    row = self.ledger.get_notification(nid)
                except Exception:
                    continue
                if row["state"] == "inflight":
                    self.ledger.defer_notification(
                        nid, next_attempt_at=now, reason="stale-prepared-reclaim")
            reclaimed += 1
        return reclaimed

    def _reconcile_existing_input(self, target, inp, sendable, now):
        """目标已有未决输入：REST 对账原 (session, msg)，绝不制造新身份。

        - 有受理/投递证据 → 补证输入与批次成员；
        - delivered 且全部成员已 ACK → 退役旧槽（freed，调用方按新槽重试该批）；
        - unknown 且尽力确认未受理 → 以原 ID 有界重试（真实 POST 仍走发送门）；
        - possible（POST 可能已发出）无证据 → 只对账延后，不盲目重发。
        """
        # 崩溃/重启遗留 prepared 槽：从未 possible（从未 POST）、owner 已变更且
        # 超过对账期限 → 可安全释放重建（经 B CAS/成员/人工/current 重验，总计只
        # POST 一次）；已 possible/unknown 严格保原 ID，不得照样释放。
        if (inp["admission_state"] == ADMISSION_PREPARED
                and inp.get("claim_owner") != self.instance):
            deadline = inp.get("reconcile_deadline")
            if not isinstance(deadline, int):
                deadline = int(inp.get("prepared_at") or inp.get("created_at") or 0) \
                    + int(self.cfg["ack_timeout"]) * 1000
            if now >= deadline:
                try:
                    rel = self.ledger.release_input_slot(
                        inp["input_id"], reason="stale-prepared-never-possible",
                        now=now, actor="supervisor")
                except Exception:
                    rel = {"released": False}
                if rel.get("released"):
                    return {"bucket": "suppressed", "count": 0, "freed": True}
            return self._defer_input_wait(target, inp, sendable, now,
                                          reason="prepared-other-owner-wait")
        sid = inp["session_id"]
        msg = inp["message_id"]
        input_id = inp["input_id"]
        ev = self._check_input_evidence(sid, msg)
        if ev == "delivered":
            if inp["admission_state"] in (ADMISSION_PREPARED, ADMISSION_POSSIBLE,
                                          ADMISSION_UNKNOWN):
                self.ledger.confirm_input_admitted(input_id,
                                                   evidence={"via": "dispatch-delivered"}, now=now)
            self.ledger.confirm_input_delivered(input_id,
                                                evidence={"via": "dispatch"}, now=now)
            # 补证后重读（confirm 会推进状态），用最新行判定已消费并退役
            fresh = self.ledger.get_input(input_id) or inp
            if self._consumed_delivered_input(fresh, now):
                if self._retire_consumed_input(fresh, now):
                    return {"bucket": "suppressed", "count": 0, "freed": True}
            # 有限催办：delivered + 业务未 ACK；dispatch 前序已把守目标 inactive、
            # inbox 空、非冷却/观察已知；等待不消耗催办预算。
            if self._nudge_delivered_input(target, fresh, sendable, now):
                return {"bucket": "sent", "count": 1}
            return self._defer_input_wait(target, fresh, sendable, now,
                                          reason="input-delivered-single-slot")
        if ev == "admitted":
            if inp["admission_state"] in (ADMISSION_PREPARED, ADMISSION_POSSIBLE,
                                          ADMISSION_UNKNOWN):
                self.ledger.confirm_input_admitted(input_id,
                                                   evidence={"via": "dispatch-inbox"}, now=now)
            return self._defer_input_wait(target, inp, sendable, now,
                                          reason="input-admitted-single-slot")
        if inp["admission_state"] == ADMISSION_UNKNOWN:
            # 只有可证明未受理（absent）且预算内，才允许以原 ID 有界重试；
            # unknown/查询失败一律只 defer + 期限告警，不 POST、不换身份。
            if (ev == "absent"
                    and (inp.get("retry_count") or 0) < int(self.cfg["dispatch_max_retries"])):
                return self._send_wake_input(
                    target, input_id=input_id, message_id=msg,
                    body=inp.get("body") or "",
                    notification_ids=inp.get("notification_ids") or [],
                    revision=int(inp.get("revision") or 1),
                    claimed=sendable, now=now)
            return self._defer_input_wait(
                target, inp, sendable, now,
                reason=("unknown-retry-budget-exhausted" if ev == "absent"
                        else "unknown-no-admission-evidence"))
        return self._defer_input_wait(target, inp, sendable, now,
                                      reason=f"input-slothold:{inp['admission_state']}")

    def reconcile_outstanding_inputs(self):
        """跨重启持续对账未决输入（即使本轮 due 为空）：只对账原 (session,msg)；
        不因 ACK/空 inbox 释放槽位；已消费 delivered+acked 槽主动退役；达到对账
        期限仍未知 → 去重可见告警；成熟孤立 prepared 槽有界回收。"""
        now = self.clock.now_ms()
        try:
            self._reclaim_stale_prepared(now)
        except Exception:
            pass
        for inp in self.ledger.list_pending_inputs(outstanding=True, limit=200):
            sid = inp.get("session_id")
            msg = inp.get("message_id")
            if not sid or not msg:
                continue
            input_id = inp["input_id"]
            if inp["admission_state"] == ADMISSION_DELIVERED:
                # 已投递：只有全部成员已 ACK 才退役；否则保持（可能待 nudge/人工）。
                if self._consumed_delivered_input(inp, now):
                    self._retire_consumed_input(inp, now)
                continue
            ev = self._check_input_evidence(sid, msg)
            if ev == "delivered":
                if inp["admission_state"] in (ADMISSION_PREPARED, ADMISSION_POSSIBLE,
                                              ADMISSION_UNKNOWN):
                    self.ledger.confirm_input_admitted(input_id,
                                                       evidence={"via": "reconcile"}, now=now)
                self.ledger.confirm_input_delivered(input_id,
                                                    evidence={"via": "reconcile"}, now=now)
                continue
            if ev == "admitted":
                if inp["admission_state"] in (ADMISSION_PREPARED, ADMISSION_POSSIBLE,
                                              ADMISSION_UNKNOWN):
                    self.ledger.confirm_input_admitted(input_id,
                                                       evidence={"via": "reconcile-inbox"}, now=now)
                continue
            if inp["admission_state"] in (ADMISSION_POSSIBLE, ADMISSION_UNKNOWN,
                                          ADMISSION_ADMITTED):
                self._input_overdue_alert(inp, now)
                self.ledger.defer_input(input_id,
                                        next_reconcile_at=now + self._wake_diagnostic_interval_ms(),
                                        reason="reconcile-wait", now=now)


    def _notify_backoff(self, items: list[dict]) -> int:
        attempts = max((n["attempts"] for n in items), default=0)
        base = int(self.cfg["notification_retry_min"])
        cap = int(self.cfg["notification_retry_max"])
        return min(cap, base * (2 ** min(attempts, 5)))

    @staticmethod
    def _rows_with_payload(rows: list[dict]) -> list[dict]:
        """finalize_input 的 text_factory 收到的是原始 SQL 行（payload 为 JSON
        字符串）；此处纯解析为 compose_wake 接受的成员行（不做任何外部 IO）。"""
        out = []
        for r in rows:
            n = dict(r)
            raw = n.get("payload")
            if isinstance(raw, str):
                try:
                    n["payload"] = json_loads(raw, {})
                except Exception:
                    n["payload"] = {}
            else:
                n["payload"] = raw if isinstance(raw, dict) else {}
            out.append(n)
        return out

    def compose_wake(self, target: str, items: list[dict]) -> str:
        lines = [
            "[oc-orch] 自动唤醒：有未消费的协调通知",
            f"目标 session：{target}",
            f"时间：{iso(self.clock.now_ms())}",
            "",
            "待处理通知（按台账处理；本消息不是验收结论）：",
        ]
        for n in items[:int(self.cfg["notification_batch"])]:
            p = n["payload"]
            lines.append(
                f"- {n['id']} kind={n['kind']} task={n.get('task_id') or '-'} "
                f"attempt={n.get('attempt_id') or '-'}"
            )
            if p.get("classification"):
                lines.append(f"  classification={p['classification']}")
            if p.get("result_version"):
                lines.append(f"  result_version={p['result_version']}")
            if p.get("summary"):
                lines.append(f"  summary={p['summary']}")
            if p.get("result_path"):
                lines.append(f"  result_path={p['result_path']}")
            if p.get("missing_deliverables"):
                lines.append(f"  missing={', '.join(map(str, p['missing_deliverables']))}")
            if p.get("stale_deliverables"):
                lines.append(f"  stale(未声明沿用)={', '.join(map(str, p['stale_deliverables']))}")
            if p.get("unverified_deliverables"):
                lines.append(f"  unverified(未执行检查)={', '.join(map(str, p['unverified_deliverables']))}")
            if p.get("ready_for_verification") is not None:
                lines.append(f"  ready_for_verification={p['ready_for_verification']}"
                             "（机器门禁；非人工验收结论）")
            if p.get("next_action"):
                lines.append(f"  next={p['next_action']}")
            if p.get("instructions"):
                lines.append(f"  instructions={p['instructions']}")
        lines += [
            "",
            "请执行：",
            "  先 oc-orch inbox --session " + target + " 读取通知与结果版本，",
            "  处理完毕后按通知 ID 显式 ack：oc-orch notify ack <ID>",
            "  （collect 不再隐式 ACK；--ack-all 仅确认本次读取到的 ID）",
            "  oc-orch status --json",
            "按状态与交付物验收；不要仅凭 execution.succeeded 或 worker 自报宣称通过。",
        ]
        return "\n".join(lines)

    def expire_notifications(self) -> None:
        expired = self.ledger.expire_notifications(
            now=self.clock.now_ms(), ack_timeout=int(self.cfg["ack_timeout"]) * 1000
        )
        for n in expired:
            self.ledger.add_alert(
                severity="high", kind="notification_unacked",
                title=f"通知投递 {n['attempts']} 次仍未确认：{n['kind']}",
                detail=json_dumps(n, ensure_ascii=False, default=str),
                dedupe_key=f"notif-expired:{n['id']}",
                task_id=n.get("task_id"), attempt_id=n.get("attempt_id"),
            )

    def handle_inbox_event(self, envelope: dict) -> None:
        """session.inbox.* 事件：只按 (session, message) 双键确认本系统输入。

        - enqueued：confirm_input_admitted 整批补证（attempts 只 +1 一次，幂等；
          之后不再逐条旧 mark_notification_sent 造成双计）；
        - delivered：先补受理再 confirm_input_delivered（遗漏 enqueued 不丢批次）；
        - cancelled：record_input_cancelled，绝不 mark_sent、不隐式 ACK 业务；
        - 会话错配/陌生消息 → None，不确认（race2 保护）；
        - 已终止/被后继替换的输入由台账端状态守卫阻止复活（runtime 不强制写）。
        """
        data = envelope.get("data") or {}
        inbox_id = data.get("inboxID")
        if not inbox_id:
            return
        etype = envelope.get("type")
        session_id = data.get("sessionID")
        now = self.clock.now_ms()
        if etype == "session.inbox.cancelled":
            # 取消只在 (session, inbox/message) 精确匹配本系统原输入时才可信；
            # 只对 source=managed_input 的显式登记输入写受管观察，绝不把
            # DELETE 成功/no-op、陌生消息或仅单键猜测当成取消证据。
            if not session_id:
                return
            inp = self.ledger.input_for_receipt(session_id, inbox_id)
            if inp is None:
                return
            if inp.get("source") == "managed_input":
                try:
                    self._managed_inputs_service().record_managed_observation(
                        inp["input_id"], cancelled=True, trusted=True,
                        via="sse:session.inbox.cancelled")
                except Exception as e:
                    self.ledger.audit(
                        "supervisor", "managed-input.cancel-rejected",
                        "session_input", inp.get("input_id"),
                        {"error": redact(repr(e), 400),
                         "session": session_id, "message": inbox_id})
                return
            self.ledger.record_input_cancelled(
                session_id=session_id, message_id=inbox_id,
                reason="cancelled", now=now, actor="supervisor")
            return
        if session_id:
            inp = self.ledger.input_for_receipt(session_id, inbox_id)
            if inp is None:
                # 外部输入可来自API、引用或自动工具。没有可信来源证明，
                # 不建真人屏障、不清任何门，也不确认本系统通知。
                return
            input_id = inp["input_id"]
            if etype == "session.inbox.delivered":
                if inp["admission_state"] in (ADMISSION_PREPARED,
                                              ADMISSION_POSSIBLE,
                                              ADMISSION_UNKNOWN):
                    self.ledger.confirm_input_admitted(
                        input_id, evidence={"via": "delivered-event"}, now=now)
                self.ledger.confirm_input_delivered(
                    input_id, evidence={"via": "delivered-event"}, now=now)
                return
            # enqueued / 未知类型 → 受理证据（整批补证；重复事件幂等）
            self.ledger.confirm_input_admitted(
                input_id, evidence={"event": etype or "enqueued"}, now=now)
            return
        # 兼容纯 legacy 夹具（旧事件无 sessionID）：单键历史回执确认；
        # 新输入一律走上面双键路径。
        row = self.ledger.notification_for_message(inbox_id)
        if row:
            self.ledger.mark_notification_sent(
                row["id"], message_id=inbox_id,
                next_attempt_at=now + int(self.cfg["ack_timeout"]) * 1000,
                owner=self.instance,
            )

    # ---- 告警输出 -------------------------------------------------
    def publish_alerts(self) -> None:
        rows = self.ledger.list_alerts(state="new", limit=50)
        if not rows:
            return
        path = os.path.join(self.home, "alerts.jsonl")
        for a in rows:
            if a.get("delivered_at"):
                continue
            line = json_dumps({
                "id": a["id"], "at": a["created_at"], "severity": a["severity"],
                "kind": a["kind"], "title": a["title"], "detail": a["detail"],
                "task_id": a.get("task_id"), "attempt_id": a.get("attempt_id"),
            })
            try:
                with open(path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
                    f.flush()
                    os.fsync(f.fileno())
                os.chmod(path, 0o600)
            except OSError:
                pass
            if self.cfg.get("desktop_alerts") and (
                os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")
            ):
                try:
                    subprocess.run(
                        ["notify-send", "-u", "critical" if a["severity"] == "high" else "normal",
                         f"[oc-orch] {a['title']}", a["detail"][:500]],
                        timeout=3, capture_output=True,
                    )
                except Exception:
                    pass
            self.ledger.mark_alert_delivered(a["id"])

    # ---- 心跳 -----------------------------------------------------
    def write_heartbeat(self, final: bool = False) -> None:
        if not self.owns_lock:
            return
        stats = self.stream.stats if not self.once else {}
        data = {
            "schema": "oc-orch.heartbeat.v1",
            "pid": os.getpid(),
            "instance": self.instance,
            "version": __version__,
            "started_at": self.started_at,
            "at": self.clock.now_ms(),
            "last_cycle_at": self.last_cycle_at,
            "connected": bool(stats.get("last_connected_at")) if stats else None,
            "stream": stats,
            "counts": self.ledger.counts(),
            "service": self.client.service.public(),
            "last_error": redact(self.last_error, 800) if self.last_error else None,
            "final": final,
        }
        try:
            atomic_write(os.path.join(self.home, HEARTBEAT_NAME),
                         json.dumps(data, ensure_ascii=False, indent=1), 0o600)
        except OSError:
            pass
        self.ledger.set_meta("last_reconcile_at", str(self.last_cycle_at or 0))
        if self.last_error:
            self.ledger.set_meta("last_error", redact(self.last_error, 800))

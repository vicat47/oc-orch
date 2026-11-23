"""oc-orch 本地 Web 控制面（entrypoints 层）。

业务写动作调用 application（composition.open_application）；每个请求独立
连接，不改变进程级环境变量。安全边界保持既有不变量：
- 默认只绑定 127.0.0.1；拒绝非 loopback Host。
- 本期不启用 token；仅允许本机 loopback 访问。
- 写请求要求 JSON Content-Type 与 `X-OC-Orch-Web: 1`，并校验 Origin。
- read_only 拒绝全部写动作。
- 取消/验收要求客户端回填对象 ID 确认。

只有 Supervisor 生命周期动作（start/stop）允许子进程
`python -m oc_orch.cli`（wheel 可用，不依赖 bin/ 布局）；其余写动作
直接调用应用，绝不通过 subprocess/redirect_stdout 执行 CLI。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import urllib.parse
from contextlib import contextmanager
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from time import monotonic

from .. import __version__
from ..application.manual_recovery import (
    register_completed_user_turn,
    release_turn_hold,
    unlock_manual_gate,
)
from ..application.registered_turn import (
    latest_user_message,
    verify_completed_user_turn,
)
from ..composition import (
    Client,
    Ledger,
    SystemClock,
    TargetGateStateError,
    connect,
    init,
    load_config,
    make_target_gate,
    open_application,
    read_session_window,
)
from ..domain.errors import ActionError, ApiError, LedgerError, OrchError, TransportError
from ..runtime.supervisor import supervisor_status
from ..util import atomic_write, ensure_home, json_dumps, now_ms
from .web_page import HTML_PAGE
from .web_assets import HTTP_WHITELIST, content_type, read_bytes

PID_NAME = "web.pid"
MAX_BODY = 1024 * 1024
SUPERVISOR_TIMEOUT = 60
# 调度者元数据（标题/创建时间/在线）缓存：自动刷新周期内不逐次查询远端服务，
# 保证排序与标题在刷新中稳定；失效时保留旧值并以 stale 标记，不重排。
_COORD_ACTIVE_TTL_S = 1  # 合并总览与工作台并发读取；短于页面的 5 秒轮询周期。
COORD_META_TTL_MS = 30_000
COORD_META_STALE_MS = 300_000
_COORD_CLIENT_TIMEOUT = 3.0  # 元数据只读查询的有界超时；仅用于本页只读探活
_COORD_OVERVIEW_TASKS_CAP = 60  # 总览卡片内联任务提示上限（全量计数不截断）


def _module_root() -> str | None:
    """子进程 `python -m oc_orch.cli` 的包源根；wheel 安装也适用。"""
    import oc_orch  # noqa: F401

    return os.path.dirname(os.path.dirname(os.path.abspath(oc_orch.__file__)))


def _clamp_limit(raw: str, *, default: int = 50, maximum: int = 200) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return max(1, min(value, maximum))


def _open_managed_inputs(home, *, client_factory=None):
    """延迟装配受管输入组合根；只读列表不构造 Client。"""
    from ..composition import open_managed_inputs

    return open_managed_inputs(home, client_factory=client_factory)


def _open_responsibilities(home):
    """延迟装配后续责任组合根；Web 只读诊断不写 SQL。"""
    from ..composition import open_responsibilities

    return open_responsibilities(home)


@contextmanager
def _open_coordinator_control(home):
    """每动作独立装配共享对话控制用例（与 CLI 同组合根）；短连接，退出即关。"""
    from ..composition import make_coordinator_control

    conn = init(ensure_home(home))
    try:
        yield make_coordinator_control(conn, SystemClock())
    finally:
        conn.close()


def _sanitize_snapshot(snap: dict, *, read_only: bool) -> dict:
    out = {
        "schema": snap.get("schema", "oc-orch.status.v1"),
        "generated_at": snap.get("generated_at"),
        "supervisor": snap.get("supervisor", {}),
        "service": snap.get("service", {}),
        "counts": snap.get("counts", {}),
        "provider_backoffs": snap.get("provider_backoffs", {}),
        "read_only": read_only,
        "home": None,
    }
    tasks = []
    for t in snap.get("tasks", []):
        tasks.append({
            "id": t.get("id"), "project": t.get("project"), "title": t.get("title"),
            "status": t.get("status"), "blocked_reason": t.get("blocked_reason"),
            "coordinator_session": t.get("coordinator_session"),
            "current_attempt": t.get("current_attempt"),
            "current_attempt_status": t.get("current_attempt_status"),
            "session_id": t.get("session_id"),
            "last_progress_at": t.get("last_progress_at"),
            "deadline_at": t.get("deadline_at"),
            "next_check_at": t.get("next_check_at"),
            "overdue": t.get("overdue"),
            "accepted_at": t.get("accepted_at"),
        })
    out["tasks"] = tasks
    atts = []
    for a in snap.get("attempts", []):
        meta = a.get("metadata") or {}
        atts.append({
            "id": a.get("id"), "task_id": a.get("task_id"),
            "generation": a.get("generation"), "session_id": a.get("session_id"),
            "directory": a.get("directory"), "status": a.get("status"),
            "dispatch_state": a.get("dispatch_state"),
            "last_progress_at": a.get("last_progress_at"),
            "deadline_at": a.get("deadline_at"),
            "terminal_at": a.get("terminal_at"),
            "terminal_kind": a.get("terminal_kind"),
            "result_path": a.get("result_path"),
            "result_summary": (a.get("result_summary") or "")[:300],
            "verification": a.get("verification"),
            "active_tool": meta.get("active_tool"),
            "active_shells": meta.get("active_shells"),
            "active_children": meta.get("active_children"),
            "missing_deliverables": meta.get("missing_deliverables"),
        })
    out["attempts"] = atts
    notes = []
    for n in (snap.get("notifications", []) + snap.get("sent_unacked", [])):
        p = n.get("payload") or {}
        notes.append({
            "id": n.get("id"), "kind": n.get("kind"), "state": n.get("state"),
            "target_session": n.get("target_session"),
            "task_id": n.get("task_id"), "attempt_id": n.get("attempt_id"),
            "attempts": n.get("attempts"), "max_attempts": n.get("max_attempts"),
            "next_attempt_at": n.get("next_attempt_at"),
            "last_error": n.get("last_error"),
            "summary": p.get("summary"), "classification": p.get("classification"),
            "missing_deliverables": p.get("missing_deliverables"),
        })
    out["notifications"] = notes
    out["alerts"] = [
        {"id": a.get("id"), "severity": a.get("severity"), "kind": a.get("kind"),
         "title": a.get("title"), "detail": (a.get("detail") or "")[:1200],
         "task_id": a.get("task_id"), "attempt_id": a.get("attempt_id"),
         "state": a.get("state"), "created_at": a.get("created_at")}
        for a in snap.get("alerts", [])
    ]
    out["leases"] = snap.get("leases", [])
    out["target_gates"] = [
        {"session_id": g.get("session_id"), "state": g.get("state"),
         "source": g.get("source"), "category": g.get("category"),
         "blocked_until": g.get("blocked_until"),
         "reason": (g.get("reason") or "")[:300],
         "last_error_at": g.get("last_error_at"),
         "revision": g.get("revision")}
        for g in snap.get("target_gates", [])
    ]
    return out


class WebServer:
    def __init__(self, home: str, *, host: str = "127.0.0.1", port: int = 8787,
                 read_only: bool = False, verbose: bool = False,
                 client_factory=None):
        self.home = ensure_home(home)
        init(self.home).close()  # 建库即关：Web 动作每请求独立连接，不持有进程级连接
        self.host = host
        self.port = int(port)
        self.read_only = read_only
        self.verbose = verbose
        self.client_factory = client_factory
        self._coord_meta: dict[str, dict] = {}
        self._coord_meta_lock = threading.Lock()
        self._coord_active: dict | None = None
        self._coord_active_until = 0.0
        self.httpd: ThreadingHTTPServer | None = None
        self.thread: threading.Thread | None = None
        self.cfg = load_config(self.home)

    def snapshot(self) -> dict:
        conn = connect(self.home)
        try:
            ledger = Ledger(conn, self.home)
            sup = supervisor_status(self.home, self.cfg)
            service = {
                "base": ledger.get_meta("service_base"),
                "version": ledger.get_meta("service_version"),
                "last_error": ledger.get_meta("last_service_error"),
            }
            snap = ledger.status_snapshot(supervisor=sup, service=service)
            try:
                snap["target_gates"] = make_target_gate(
                    conn, SystemClock()).list_all()
            except TargetGateStateError as e:
                # 闸门记录损坏不能拖垮整个控制面；作为可见状态暴露。
                snap["target_gates"] = [{
                    "session_id": None, "state": "state-error",
                    "reason": str(e)[:200],
                }]
            out = _sanitize_snapshot(snap, read_only=self.read_only)
            out["home"] = self.home
            out["now"] = now_ms()
            return out
        finally:
            conn.close()

    def coordinator_titles(self) -> dict:
        """读取协调者 session 的标题；只读，服务不可用时回退为空映射。"""
        conn = connect(self.home)
        try:
            ledger = Ledger(conn, self.home)
            ids = set()
            for task in ledger.list_tasks(limit=200):
                sid = task.get("coordinator_session")
                if sid:
                    ids.add(sid)
            for lease in ledger.list_coordinator_leases():
                sid = lease.get("holder_session")
                if sid:
                    ids.add(sid)
        finally:
            conn.close()
        if not ids:
            return {}
        try:
            client = (self.client_factory or Client)()
        except Exception:
            return {}
        titles = {}
        for sid in sorted(ids):
            try:
                session = client.get_session(sid)
            except Exception:
                continue
            title = (session or {}).get("title")
            if isinstance(title, str) and title.strip():
                titles[sid] = title.strip()
        return titles

    def audit(self, limit: int = 50) -> list[dict]:
        conn = connect(self.home)
        try:
            return Ledger(conn, self.home).list_audit(max(1, min(limit, 500)))
        finally:
            conn.close()

    def managed_inputs(self, *, session=None, limit=50):
        """只读本地台账列表；不构造 Client、不触发远端观察/reconcile。"""
        with _open_managed_inputs(self.home,
                                  client_factory=self.client_factory) as mi:
            return mi.list_inputs(session_id=session, limit=limit)

    def managed_watches(self, *, limit=50):
        """只读白名单列表；不构造 Client、不触发远端 observe。"""
        with _open_managed_inputs(self.home,
                                  client_factory=self.client_factory) as mi:
            return mi.list_watches(limit=limit)

    def responsibilities(self, *, task_id=None, owner=None, limit=100):
        """只读未关闭后续责任；不发送、不转交、不关闭。"""
        with _open_responsibilities(self.home) as store:
            return {"ok": True,
                    "responsibilities": store.list_open_responsibilities(
                        task_id=task_id, owner=owner, limit=limit)}

    # ---- R2 业务健康 / 项目控制 / 本地人工待办（共享应用用例接线）------------
    def continuity_health(self, *, project=None, session_id=None,
                          limit=50, after_id=None) -> dict:
        """本地只读业务健康投影；进程健康仅展示、不推进游标、不构造 Client。

        与 CLI ``health`` 共用同一应用用例（composition.make_continuity_status），
        逐任务附 process 健康与独立控制维度；scope 参数错误由共享用例稳定断言。
        """
        conn = connect(self.home)
        try:
            from ..composition import make_continuity_status

            ledger = Ledger(conn, self.home)
            sup = supervisor_status(self.home, self.cfg)
            process = {k: sup.get(k) for k in (
                "healthy", "connected", "heartbeat_age_s", "last_cycle_at")}
            return make_continuity_status(ledger, SystemClock()).snapshot(
                project=project, session_id=session_id, limit=limit,
                after_id=after_id, process=process, advance_cursor=False)
        finally:
            conn.close()

    def incidents(self, *, project=None, limit=50, after_id=None) -> dict:
        """本地人工待办脱敏投影；只 read，不 reconcile、不主动发邮件。"""
        conn = connect(self.home)
        try:
            from ..composition import make_configured_operator_incident

            service = make_configured_operator_incident(
                Ledger(conn, self.home), home=self.home)
            return service.snapshot(project=project, limit=limit, after_id=after_id)
        finally:
            conn.close()

    def delivery_policy(self, *, task_id=None, project=None) -> dict:
        from ..application.delivery_policy_control import DeliveryPolicyControl
        from ..composition import make_delivery_policy

        conn = connect(self.home)
        try:
            out = DeliveryPolicyControl(make_delivery_policy(Ledger(conn, self.home))).show(
                task_id=task_id, project=project)
            return {**out, "read_only": self.read_only}
        finally:
            conn.close()

    def delivery_policy_apply(self, scope, body) -> dict:
        from ..application.delivery_policy_control import DeliveryPolicyControl
        from ..composition import make_delivery_policy

        conn = connect(self.home)
        try:
            return DeliveryPolicyControl(make_delivery_policy(Ledger(conn, self.home))).apply(
                scope, body, actor="operator:web")
        finally:
            conn.close()

    def project_control_view(self, *, project, session_id, operation_id=None) -> dict:
        from ..composition import make_project_control_view

        conn = connect(self.home)
        try:
            return {**make_project_control_view(conn).show(
                project=project, session_id=session_id, operation_id=operation_id),
                "read_only": self.read_only}
        finally:
            conn.close()

    def session_control_view(self, session_id: str) -> dict:
        """只读 session 回合控制快照；损坏/不可读时明确未知，不当作无 hold。"""
        try:
            with _open_coordinator_control(self.home) as control:
                snap = control.get_session(session_id)
        except Exception as exc:
            return {"ok": False, "error": type(exc).__name__}
        return {"ok": True, "revision": snap["revision"],
                "held": snap["hold"] is not None, "hold": snap["hold"],
                "user_turn_active": snap["barrier"] is not None,
                "barrier": snap["barrier"], "watermark": snap["watermark"]}

    def turn_candidate(self, *, project: str, session_id: str) -> dict:
        """只读预览“最近一个已完成用户回合”是否可登记（不返回聊天正文）。

        必须先通过租约校验；候选只取最新用户消息，且用与登记相同的窗口
        核验函数验证；不可登记时返回稳定 reason，不在 UI 侧推断。
        """
        with _open_coordinator_control(self.home) as control:
            lease = control.coordinator_lease(project)
            if not lease or lease.get("holder_session") != session_id:
                raise LedgerError("TURN_CANDIDATE_AUTHORITY",
                                  "session 不是该项目当前协调者")
            snap = control.get_session(session_id)
            if snap.get("barrier") is not None:
                return {"ok": True, "eligible": False,
                        "reason": "user-turn-active", "revision": snap["revision"],
                        "hold": snap.get("hold")}
            hold = snap.get("hold")
            if not hold:
                return {"ok": True, "eligible": False,
                        "reason": "no-active-hold", "revision": snap["revision"],
                        "hold": None}
            if hold.get("reason") not in ("coordinator-interrupted",):
                return {"ok": True, "eligible": False,
                        "reason": "hold-not-releasable",
                        "revision": snap["revision"], "hold": hold}
            client = self._coord_client()
            latest = latest_user_message(client, session_id)
            if not latest.get("found"):
                return {"ok": True, "eligible": False,
                        "reason": "no-user-input", "revision": snap["revision"],
                        "hold": hold}
            conn = connect(self.home)
            try:
                ledger = Ledger(conn, self.home)
                result = verify_completed_user_turn(
                    control=control, ledger=ledger, client=client,
                    window=lambda sid, **kw: read_session_window(
                        client, sid, max_pages=20, page_size=50, **kw),
                    clock=SystemClock(), session_id=session_id,
                    message_id=latest["message_id"])
            finally:
                conn.close()
            out = {"ok": True, "revision": snap["revision"], "hold": hold,
                   "message_id": latest["message_id"],
                   "created": latest.get("created")}
            if not result.get("finished"):
                out.update(eligible=False, reason=result.get("reason"))
                return out
            out.update(eligible=True, reason=result.get("reason"),
                       idle_id=result.get("idle_id"),
                       idle_at=result.get("idle_at"),
                       outcome=result.get("outcome"))
            return out

    def release_hold(self, body: dict) -> dict:
        """显式运维解除 session 回合 hold；共享 application 用例，不在 Web 自写规则。"""
        if self.read_only:
            raise LedgerError("READ_ONLY", "只读控制面拒绝写动作")
        project = str(body.get("project") or "").strip()
        session_id = str(body.get("session_id") or "").strip()
        operation_id = str(body.get("operation_id") or "").strip()
        reason = str(body.get("reason") or "").strip()
        if not project or not session_id or not operation_id or not reason:
            raise LedgerError("INVALID_SCOPE", "缺少 project/session_id/operation_id/reason")
        if body.get("confirm") != project:
            raise LedgerError("CONFIRM_REQUIRED", f"需输入与项目完全一致的确认文本：{project}")
        revision = body.get("expected_revision")
        epoch = body.get("expected_epoch")
        if type(revision) is not int or revision < 0:
            raise LedgerError("INVALID_REVISION", "缺少 expected_revision（int >= 0）")
        if type(epoch) is not int or epoch < 1:
            raise LedgerError("INVALID_EPOCH", "缺少 expected_epoch（int >= 1）")
        evidence = body.get("evidence")
        if evidence is not None and not isinstance(evidence, dict):
            raise LedgerError("INVALID_EVIDENCE", "evidence 必须为 JSON 对象或省略")
        conn = connect(self.home)
        try:
            ledger = Ledger(conn, self.home)
            with _open_coordinator_control(self.home) as control:
                out = release_turn_hold(
                    control, ledger, project=project, session=session_id,
                    operation_id=operation_id, reason=reason,
                    expected_revision=revision, expected_epoch=epoch,
                    evidence=evidence, actor="web", now=now_ms())
        finally:
            conn.close()
        return {**out, "session_control": self.session_control_view(session_id)}

    def register_completed_turn(self, body: dict) -> dict:
        """登记已完成的真实用户回合；窗口核验后一次性 begin+finish。"""
        if self.read_only:
            raise LedgerError("READ_ONLY", "只读控制面拒绝写动作")
        project = str(body.get("project") or "").strip()
        session_id = str(body.get("session_id") or "").strip()
        operation_id = str(body.get("operation_id") or "").strip()
        reason = str(body.get("reason") or "").strip()
        message_id = str(body.get("message_id") or "").strip()
        if not project or not session_id or not operation_id or not reason or not message_id:
            raise LedgerError("INVALID_SCOPE",
                              "缺少 project/session_id/operation_id/reason/message_id")
        if body.get("confirm") != project:
            raise LedgerError("CONFIRM_REQUIRED", f"需输入与项目完全一致的确认文本：{project}")
        revision = body.get("expected_revision")
        epoch = body.get("expected_epoch")
        if type(revision) is not int or revision < 0:
            raise LedgerError("INVALID_REVISION", "缺少 expected_revision（int >= 0）")
        if type(epoch) is not int or epoch < 1:
            raise LedgerError("INVALID_EPOCH", "缺少 expected_epoch（int >= 1）")
        client = self._coord_client()
        conn = connect(self.home)
        try:
            ledger = Ledger(conn, self.home)
            with _open_coordinator_control(self.home) as control:
                def verify(sid: str, mid: str) -> dict:
                    return verify_completed_user_turn(
                        control=control, ledger=ledger, client=client,
                        window=lambda s, **kw: read_session_window(
                            client, s, max_pages=20, page_size=50, **kw),
                        clock=SystemClock(), session_id=sid, message_id=mid)
                out = register_completed_user_turn(
                    control, ledger, verify, project=project, session=session_id,
                    operation_id=operation_id, reason=reason,
                    expected_revision=revision, expected_epoch=epoch,
                    message_id=message_id, actor="web", now=now_ms())
        finally:
            conn.close()
        return {**out, "session_control": self.session_control_view(session_id)}

    def prototype(self, task_id, body=None, operation_id=None):
        from ..composition import make_prototype

        conn = connect(self.home)
        try:
            service = make_prototype(Ledger(conn, self.home))
            if body is not None:
                return service.apply(task_id, body, actor="operator:web")
            return {**service.show(task_id, operation_id), "read_only": self.read_only}
        finally:
            conn.close()

    def coordinator_control(self, action: str, body: dict) -> dict:
        """项目暂停/继续：经共享 ConversationControl 用例，不自写控制规则。

        - 必填 project/session_id/operation_id/reason/expected_revision/
          expected_epoch；confirm 必须与 project 逐字一致；
        - 不接收/不转发任何“trusted/attestation”请求字段——来源固定为本地
          operator 调用层的常量（与 CLI 同一标签），HTTP/metadata 永不接成该值；
        - CAS/旧 holder/多项目守卫全部由共享用例负责，service 拒绝即如实上报。
        """
        if action not in ("pause", "resume"):
            raise ActionError("usage", "未知项目控制动作")
        for field in ("project", "session_id", "operation_id", "reason"):
            value = body.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ActionError("usage", f"{field} 必须为非空字符串")
        project = body["project"]
        if project != project.strip():
            raise ActionError("usage", "project 必须为原样项目身份，不隐式清理空白")
        session_id = body["session_id"]
        operation_id = body["operation_id"]
        reason = body["reason"]
        expected_revision = body.get("expected_revision")
        expected_epoch = body.get("expected_epoch")
        confirm = body.get("confirm")
        if type(expected_epoch) is not int or expected_epoch < 1:
            raise ActionError("usage", "缺少 expected_epoch（int >= 1）")
        if type(expected_revision) is not int or expected_revision < 0:
            raise ActionError("usage", "缺少 expected_revision（int >= 0）")
        if not isinstance(confirm, str) or confirm != project:
            raise WebActionError(
                "CONFIRM_REQUIRED", f"需输入与项目完全一致的确认文本：{project}")
        evidence = body.get("evidence")
        if evidence is not None and not isinstance(evidence, dict):
            raise ActionError("usage", "evidence 必须为 JSON 对象或省略")
        from ..application.conversation_control import ConversationControl
        with _open_coordinator_control(self.home) as control:
            kw = dict(project=project, session_id=session_id,
                      operation_id=operation_id, reason=reason,
                      expected_revision=expected_revision,
                      expected_epoch=expected_epoch, evidence=evidence)
            out = (ConversationControl(control).pause(**kw) if action == "pause"
                   else ConversationControl(control).resume(**kw))
        return {"ok": out.get("ok"), "executed": out.get("executed"),
                "action": out.get("action"), "project": out.get("project"),
                "session_id": out.get("session_id"), "result": out.get("result")}

    def operator_turn(self, action, body):
        """指定的显式补充指令入口；不接受trusted/任意消息ID或finish断言。"""
        from ..composition import make_operator_turn

        conn = connect(self.home)
        try:
            service = make_operator_turn(Ledger(conn, self.home), client=self._coord_client)
            if action == "send":
                return service.send(body)
            if set(body) != {"project", "session_id", "operation_id"}:
                raise LedgerError("OPERATOR_TURN_INVALID", "只接受精确项目/session/原operation")
            return service.show(**body) if action == "show" else service.observe(**body)
        finally:
            conn.close()

    # ---- 调度者入口只读聚合（coordinator-first Web）--------------------
    # 只读 API：通过 ledger/adapter 读方法与有界元数据缓存聚合，不改领域状态，
    # 不逐次远程查询（自动刷新期间排序稳定）。创建时间唯一可靠来源是 OpenCode
    # V2 session 对象 time.created；台账无创建时间列，缺失/失败时明确显示未知，
    # 采用 session ID 稳定兜底排序，绝不从 ID 推断创建时间。

    def _coord_client(self):
        """只读元数据探活的客户端；缺省用有界超时 Client，测试可注入 factory。
        经由 composition 再导出，保持 entrypoints 不直接接触 adapters。"""
        if self.client_factory is not None:
            return self.client_factory()
        return Client(timeout=_COORD_CLIENT_TIMEOUT)

    def _coord_activity(self, client) -> dict | None:
        """锁内共享最近一秒的前台执行快照，失败时丢弃旧快照。"""
        if monotonic() >= self._coord_active_until:
            try:
                self._coord_active = client.active_sessions()
            except Exception:
                self._coord_active = None
            self._coord_active_until = monotonic() + _COORD_ACTIVE_TTL_S
        return self._coord_active

    def _coord_meta_ensure(self, client, sids: list[str], now: int) -> tuple[dict, dict | None]:
        """按长 TTL 补取标题、创建时间，同时为所有会话读取同一份前台执行状态。"""
        if not sids:
            return {}, None
        with self._coord_meta_lock:
            out: dict[str, dict] = {}
            to_fetch = []
            for sid in sids:
                ent = self._coord_meta.get(sid)
                if ent and now - int(ent.get("fetched_at") or 0) < COORD_META_TTL_MS:
                    out[sid] = ent
                else:
                    to_fetch.append(sid)
            active = self._coord_activity(client)
            for sid in to_fetch:
                prev = self._coord_meta.get(sid) or {}
                if active is None:
                    ent = {
                        "title": prev.get("title"),
                        "created_at": prev.get("created_at"),
                        "fetched_at": now,
                        "error": prev.get("error") or "active-unknown",
                        "missing": prev.get("missing", False),
                    }
                    self._coord_meta[sid] = ent
                    out[sid] = ent
                    continue
                try:
                    s = client.get_session(sid)
                except Exception as e:
                    ent = {
                        "title": prev.get("title"),
                        "created_at": prev.get("created_at"),
                        "fetched_at": now,
                        "error": f"{type(e).__name__}: {str(e)[:160]}",
                        "missing": prev.get("missing", False),
                    }
                    self._coord_meta[sid] = ent
                    out[sid] = ent
                    continue
                if not isinstance(s, dict):
                    ent = {"title": None, "created_at": None,
                           "fetched_at": now, "error": None, "missing": True}
                else:
                    times = s.get("time") or {}
                    t = times.get("created") if isinstance(times, dict) else None
                    created = int(t) if isinstance(t, int) and t > 0 else None
                    title = s.get("title")
                    if not isinstance(title, str) or not title.strip():
                        title = None
                    ent = {"title": title, "created_at": created,
                           "fetched_at": now, "error": None, "missing": False}
                self._coord_meta[sid] = ent
                out[sid] = ent
            # 大量标题首次读取可能较慢，返回前保证执行状态仍是近期采样。
            return out, self._coord_activity(client) if to_fetch else active

    @staticmethod
    def _session_run_state(sid: str, active: dict | None, metadata_fresh: bool) -> str:
        if active is None:
            return "unknown"
        if sid in active:
            return "running" if active[sid].get("type") == "running" else "unknown"
        return "inactive" if metadata_fresh else "unknown"

    def _coordinator_discovery(self) -> dict:
        """只读发现全量已知调度者候选集合及全量任务/租约/通知事实。"""
        conn = connect(self.home)
        try:
            ledger = Ledger(conn, self.home)
            leases_all = ledger.list_coordinator_leases()
            lease_by_project: dict[str, dict] = {}
            for l in leases_all:
                if l.get("project") is not None:
                    lease_by_project[l["project"]] = l
            lease_holders = sorted({l["holder_session"] for l in leases_all
                                    if l.get("holder_session")})
            bound = ledger.distinct_coordinator_sessions()
            targets = ledger.notification_target_sessions()
            target_counts = ledger.notification_target_counts()
            sids = sorted(set(lease_holders) | set(bound) | set(targets))
            tasks_all = ledger.task_coordinator_rows()
            atts = {a["id"]: a for a in ledger.list_attempts(limit=2000)}
            gates_all: dict[str, dict] = {}
            try:
                gates_all = {g["session_id"]: g for g in make_target_gate(
                    conn, SystemClock()).list_all() if g.get("session_id")}
            except TargetGateStateError:
                gates_all = {}
            return {
                "ledger": ledger, "conn": conn,
                "leases_all": leases_all, "lease_by_project": lease_by_project,
                "lease_holders": lease_holders, "bound": bound, "targets": targets,
                "target_counts": target_counts, "sids": sids,
                "tasks_all": tasks_all, "atts": atts, "gates_all": gates_all,
            }
        finally:
            conn.close()

    @staticmethod
    def _effective_coordinator(t, lease_by_project: dict[str, dict]) -> str | None:
        """与 supervisor.coordinator_for 一致的归属：显式绑定优先，否则项目租约。"""
        if t.get("coordinator_session"):
            return t["coordinator_session"]
        lease = lease_by_project.get(t.get("project"))
        return lease["holder_session"] if lease and lease.get("holder_session") else None

    def _attention(self, *, gates: list[dict], tasks: list[dict], pending_notes: int,
                   stalled: bool) -> tuple[int, str]:
        """最高优先关注原因（rank 越小越优先）。"""
        worst: list[tuple[int, str]] = []
        gate_labels = {
            "exhausted": "重试次数已用完", "probe-unknown": "探测结果未知",
            "blocked-unknown": "阻塞原因未知", "cooling": "调度正在冷却", "blocked": "调度暂时受阻",
        }
        for g in gates:
            st = g.get("state")
            if st == "manual-blocked":
                worst.append((0, "等待人工解除调度限制"))
            elif st in ("exhausted", "probe-unknown", "blocked-unknown"):
                worst.append((1, gate_labels[st]))
            elif st in ("cooling", "blocked"):
                worst.append((2, gate_labels[st]))
        for t in tasks:
            title = t.get("title") or t["id"]
            if t.get("status") == "blocked":
                reason = t.get("blocked_reason")
                worst.append((2, f"{title} · {reason or '任务受阻'}"))
            elif t.get("status") in ("stalled",):
                worst.append((2, f"{title} · 进展停滞"))
            elif t.get("status") == "result_pending":
                worst.append((3, f"{title} · 等待收集或验收"))
        if pending_notes > 0:
            worst.append((4, f"{pending_notes} 条通知等待投递或确认"))
        if stalled:
            worst.append((5, "项目租约等待恢复"))
        if not worst:
            return (6, "无待办关注")
        worst.sort(key=lambda x: x[0])
        rank, text = worst[0]
        return rank, text

    def coordinator_overview(self) -> dict:
        """总览：全部已知调度者 + 稳定排序 + 明确范围/未知标注。"""
        now = now_ms()
        d = self._coordinator_discovery()
        sids = d["sids"]
        coord_tasks: dict[str, list[dict]] = {s: [] for s in sids}
        unbound: list[dict] = []
        for t in d["tasks_all"]:
            c = self._effective_coordinator(t, d["lease_by_project"])
            if c and c in coord_tasks:
                coord_tasks[c].append(t)
            else:
                unbound.append(t)
        # 元数据（含创建时间/标题/在线）：一次性有界取数与缓存
        meta_ok = False
        meta_error: str | None = None
        entries: dict[str, dict] = {}
        try:
            client = self._coord_client()
            meta, active = self._coord_meta_ensure(client, sids, now)
            meta_ok = True
        except Exception as e:  # 构造失败/全局失败：全部视为未知，不重排
            meta, active = {}, None
            meta_error = f"{type(e).__name__}: {str(e)[:160]}"
        for sid in sids:
            ent = meta.get(sid) or {}
            fetched_age = now - int(ent.get("fetched_at") or 0)
            # 每入口独立新鲜度：error/missing/老化 → 非 fresh；active 布尔只在
            # fresh 时才是最新事实，否则归为“在线状态未知”，不做陈旧事实冒充。
            fresh = (not ent.get("error") and not ent.get("missing")
                     and fetched_age <= COORD_META_TTL_MS)
            entries[sid] = {**ent, "_fetched_age_ms": fetched_age, "_fresh": fresh}
        targets_set = set(d["targets"])
        coordinators = []
        known = unknown = 0
        for sid in sids:
            ent = entries[sid]
            fresh = ent["_fresh"]
            created = ent.get("created_at")
            known_flag = isinstance(created, int) and created > 0
            if known_flag:
                known += 1
            else:
                unknown += 1
            owns = coord_tasks[sid]
            gates = [g for g in d["gates_all"].values() if g.get("session_id") == sid]
            pending_notes = int(d["target_counts"].get(sid) or 0)
            unfinished = [t for t in owns if t.get("status") not in ("accepted", "cancelled")]
            accepted = [t for t in owns if t.get("status") == "accepted"]
            cancelled = [t for t in owns if t.get("status") == "cancelled"]
            awaiting = [t for t in unfinished if t.get("status") in
                        ("result_pending", "result_received", "result_collected")]
            blocked = [t for t in unfinished if t.get("status") in ("blocked", "stalled")]
            rank, attention = self._attention(
                gates=gates, tasks=unfinished, pending_notes=pending_notes,
                stalled=bool([l for l in d["leases_all"]
                              if l.get("holder_session") == sid
                              and (l.get("failure_count") or 0) > 0]))
            created_stale = (bool(ent.get("error") or ent.get("missing"))
                             or ent["_fetched_age_ms"] > COORD_META_STALE_MS)
            last_update = max([int(t.get("updated_at") or 0)
                               for t in owns] + [int(l.get("renewed_at") or 0)
                                                 for l in d["leases_all"]
                                                 if l.get("holder_session") == sid]
                              + [0])
            coordinators.append({
                "session": sid,
                "title": ent.get("title"),
                "created_at": created if known_flag else None,
                "created_at_known": known_flag,
                "created_meta_error": ent.get("error"),
                "created_meta_missing": bool(ent.get("missing")),
                "created_meta_stale": created_stale,
                "metadata_fresh": fresh,
                "online": sid in active if fresh and active is not None else None,
                "session_run_state": self._session_run_state(sid, active, fresh),
                "projects": sorted({t.get("project") for t in owns
                                    if t.get("project")}
                                   | {l.get("project") for l in d["leases_all"]
                                      if l.get("holder_session") == sid
                                      and l.get("project")}),
                "leases": [{
                    "project": l.get("project"), "expires_at": l.get("expires_at"),
                    "renewed_at": l.get("renewed_at"),
                    "failure_count": l.get("failure_count") or 0,
                    "recovery_blocked": bool(l.get("recovery_blocked")),
                    "valid": bool(l.get("expires_at") and int(l["expires_at"]) > now),
                } for l in d["leases_all"] if l.get("holder_session") == sid],
                "gates": [{
                    "state": g.get("state"), "source": g.get("source"),
                    "category": g.get("category"),
                    "blocked_until": g.get("blocked_until"), "reason": g.get("reason"),
                } for g in gates],
                "counts": {
                    "unfinished": len(unfinished),
                    "running": sum(t.get("status") == "running" for t in unfinished),
                    "awaiting_verification": len(awaiting),
                    "blocked": len(blocked),
                    "accepted": len(accepted),
                    "cancelled": len(cancelled),
                    "notifications_pending": pending_notes,
                },
                "task_tips": [{
                    "id": t["id"], "project": t.get("project"),
                    "title": t.get("title"), "status": t.get("status"),
                    "blocked_reason": t.get("blocked_reason"),
                    "current_attempt": t.get("current_attempt_id"),
                    "current_attempt_status":
                        (d["atts"].get(t.get("current_attempt_id")) or {}).get("status"),
                    "updated_at": t.get("updated_at"),
                } for t in sorted(unfinished, key=lambda x: -int(x.get("updated_at") or 0))
                    [:_COORD_OVERVIEW_TASKS_CAP]],
                "task_tips_truncated": len(unfinished) > _COORD_OVERVIEW_TASKS_CAP,
                "last_update": last_update or None,
                "attention_rank": rank,
                "attention": attention,
                # 来源：lease/task 为调度者相关证据；notify 依据全量通知历史发现集合
                # （含已 ACK/历史，与 DISTINCT 发现一致；pending 计数 target_counts 独立）
                "sources": sorted(
                    [f"lease:{l['project']}" for l in d["leases_all"]
                     if l.get("holder_session") == sid and l.get("project")]
                    + [f"task:{t['id']}" for t in d["tasks_all"]
                       if t.get("coordinator_session") == sid]
                    + ([f"notify:{sid}"] if sid in targets_set else []),
                ),
                # 身份级别：has_task_lease = 有任务绑定/租约证据 → 可确认调度者关联；
                # notify-only（含 worker/kind=test 或历史 acked 目标）→ 仅通知关联，
                # “调度者角色未确认”；两者皆无 → unknown。UI 据此展示，不把未知身份当确认 coordinator。
                "identity": ("confirmed" if any(
                    s.startswith("lease:") or s.startswith("task:") for s in (
                        [f"lease:{l['project']}" for l in d["leases_all"]
                         if l.get("holder_session") == sid and l.get("project")]
                        + [f"task:{t['id']}" for t in d["tasks_all"]
                           if t.get("coordinator_session") == sid]))
                             else ("notify-associated" if sid in targets_set else "unknown")),
            })
        # 稳定排序：可靠创建时间从旧到新，缺失/未知排在已知之后，session ID 兜底。
        coordinators.sort(key=lambda c: (
            c["created_at"] if c["created_at_known"] else float("inf"), c["session"]))
        summary = {
            "scope": "总览 = 任务显式绑定/项目租约**确认的调度者** ∪ 通知目标**关联会话**（notification.target_session 全量 DISTINCT；仅构成关联证据，不证明当前调度角色/在线/责任，worker 或 kind=test 等目标也可能出现，页面以 identity 标注角色未确认）；非全体 OpenCode session；通知 pending 计数独立于来源，不与身份确认等价",
            "order": "可靠创建时间从旧到新；未知创建时间排后并以 session ID 兜底（明确标注未知，不推断）",
            "created_at_known": known,
            "created_at_unknown": unknown,
            # 元数据整体新鲜 = 取数成功 且 每个入口均 fresh（无 error/missing/老化）
            # 且所有创建时间已知；任一 session 单体查询失败即整体非 fresh。
            "metadata_fresh": bool(meta_ok) and all(
                entries[s]["_fresh"] for s in sids) and known == len(sids),
            "metadata_error": meta_error,
            "metadata_ttl_ms": COORD_META_TTL_MS,
            "session_count": len(sids),
            "unbound_task_count": len(unbound),
            "unbound_tasks": [{
                "id": t["id"], "project": t.get("project"), "title": t.get("title"),
                "status": t.get("status"), "blocked_reason": t.get("blocked_reason"),
            } for t in sorted(unbound, key=lambda x: -int(x.get("updated_at") or 0))][:20],
            "unbound_truncated": len(unbound) > 20,
            "task_counts_note": "任务聚合基于全量台账（不被快照 200 项截断）；"
                                "内联任务提示最多 %d 项" % _COORD_OVERVIEW_TASKS_CAP,
        }
        return {"ok": True, "generated_at": now, "meta": summary,
                "coordinators": coordinators}

    def coordinator_detail(self, session_id: str, *, after_id: str | None = None,
                            limit: int = 200) -> dict:
        """单个调度者工作台：任务/待办/活动分区与诊断所需事实（只读）。

        任务列表按 **任务 ID 稳定升序** keyset 分页（after_id 续页），有范围明示，
        不为方便无界输出；unfinished_full_count/tasks_full_count 为全量计数。
        """
        d = self._coordinator_discovery()
        if session_id not in set(d["sids"]):
            raise LedgerError("COORDINATOR_NOT_FOUND",
                              f"调度者不在当前发现范围：{session_id}")
        # 复用总览同一聚合项，保证与卡片事实一致
        overview = self.coordinator_overview()
        item = next((c for c in overview["coordinators"]
                     if c["session"] == session_id), None)
        owns = []
        for t in d["tasks_all"]:
            c = self._effective_coordinator(t, d["lease_by_project"])
            if c == session_id:
                owns.append(t)

        def _task_view(t: dict) -> dict:
            cur = d["atts"].get(t.get("current_attempt_id"))
            return {
                "id": t["id"], "project": t.get("project"), "title": t.get("title"),
                "status": t.get("status"), "blocked_reason": t.get("blocked_reason"),
                "coordinator_session": t.get("coordinator_session"),
                "owned_via": "explicit" if t.get("coordinator_session") == session_id
                             else "lease",
                "current_attempt": cur["id"] if cur else None,
                "current_attempt_status": cur["status"] if cur else None,
                "session_id": cur.get("session_id") if cur else None,
                "generation": cur.get("generation") if cur else None,
                "updated_at": t.get("updated_at"),
                "deadline_at": cur.get("deadline_at") if cur else None,
            }

        lim = max(1, min(int(limit), 500))
        owns_sorted = sorted(owns, key=lambda t: t["id"])
        page_rows = ([t for t in owns_sorted if t["id"] > after_id]
                     if after_id is not None else list(owns_sorted))[:lim + 1]
        truncated = len(page_rows) > lim
        page_rows = page_rows[:lim]
        next_after_id = page_rows[-1]["id"] if truncated and page_rows else None
        tasks = [_task_view(t) for t in page_rows]
        unfinished_full = [t for t in owns
                           if t.get("status") not in ("accepted", "cancelled")]
        # 通知用独立请求连接读取（发现快照用的连接已按约定关闭）
        conn_n = connect(self.home)
        try:
            notes = Ledger(conn_n, self.home).list_notifications(
                target_session=session_id, limit=500)
        finally:
            conn_n.close()
        resp = []
        with _open_responsibilities(self.home) as store:
            for r in store.list_open_responsibilities(limit=1000):
                if r.get("owner") == session_id or r.get("target_session") == session_id:
                    resp.append(r)
        return {"ok": True, "coordinator": item,
                "session_control": self.session_control_view(session_id),
                "tasks": tasks, "tasks_full_count": len(owns),
                "unfinished_full_count": len(unfinished_full),
                "next_after_id": next_after_id, "tasks_truncated": truncated,
                "notifications": [{
                    "id": n.get("id"), "kind": n.get("kind"), "state": n.get("state"),
                    "message_id": n.get("message_id"),
                    "task_id": n.get("task_id"), "attempt_id": n.get("attempt_id"),
                    "created_at": n.get("created_at"),
                    "sent_at": n.get("sent_at"),
                    "attempts": n.get("attempts"), "max_attempts": n.get("max_attempts"),
                    "next_attempt_at": n.get("next_attempt_at"),
                    "summary": (n.get("payload") or {}).get("summary"),
                    "classification": (n.get("payload") or {}).get("classification"),
                    "missing_deliverables": (n.get("payload") or {}).get(
                        "missing_deliverables"),
                } for n in notes],
                "responsibilities": resp,
                "tasks_scope": ("全量台账任务，按任务 ID 稳定升序 keyset 分页；"
                                "本页 %d 项 / 全量 %d 项，可传 after_id 继续加载"
                                % (len(tasks), len(owns))),
            }

    def task_detail(self, task_id: str) -> dict:
        """任务详情（只读）：任务 / 当前尝试 / 历史尝试与验收摘要。"""
        conn = connect(self.home)
        try:
            ledger = Ledger(conn, self.home)
            task = ledger.get_task(task_id)
            # 有效协调者：显式 coordinator_session 优先，否则同项目租约 holder
            eff = task.get("coordinator_session") or None
            coord_source = "explicit" if eff else None
            if not eff:
                lease = ledger.coordinator_lease(task.get("project") or "")
                if lease and lease.get("holder_session"):
                    eff = lease["holder_session"]
                    coord_source = "lease"
            attempts = ledger.list_attempts(task_id=task_id, limit=50)
            att_objs = []
            for a in attempts:
                att_objs.append({
                    "id": a["id"], "generation": a["generation"],
                    "session_id": a.get("session_id"), "status": a["status"],
                    "dispatch_state": a.get("dispatch_state"),
                    "created_at": a.get("created_at"),
                    "started_at": a.get("started_at"),
                    "last_progress_at": a.get("last_progress_at"),
                    "terminal_at": a.get("terminal_at"),
                    "terminal_kind": a.get("terminal_kind"),
                    "result_collected_at": a.get("result_collected_at"),
                    "result_path": a.get("result_path"),
                    "result_summary": (a.get("result_summary") or "")[:1500],
                    "verification": a.get("verification") if isinstance(
                        a.get("verification"), dict) else {},
                    "active_tool": (a.get("metadata") or {}).get("active_tool"),
                    "active_shells": (a.get("metadata") or {}).get("active_shells"),
                    "missing_deliverables": (a.get("metadata") or {}).get(
                        "missing_deliverables"),
                })
            notes = ledger.list_notifications_for_task(task_id, limit=50)
            from ..composition import make_delivery_policy

            try:
                delivery_policy = make_delivery_policy(ledger).task(task_id)
                project_policy = make_delivery_policy(ledger).project(task["project"])
            except LedgerError as exc:
                delivery_policy = {"ok": False, "code": exc.code, "message": str(exc)}
                project_policy = None
            try:
                from ..composition import make_prototype

                prototype = make_prototype(ledger).show(task_id)
            except LedgerError as exc:
                prototype = {"ok": False, "code": exc.code}
            from ..composition import make_failover_snapshot

            failover_status = make_failover_snapshot(conn, task_id=task_id)
            return {
                "ok": True,
                "failover": failover_status,
                "delivery_policy": delivery_policy,
                "project_delivery_policy": project_policy,
                "prototype": prototype,
                "task": {
                    "id": task["id"], "project": task.get("project"),
                    "title": task.get("title"), "objective": task.get("objective"),
                    "acceptance": task.get("acceptance"),
                    "status": task.get("status"),
                    "blocked_reason": task.get("blocked_reason"),
                    "coordinator_session": task.get("coordinator_session"),
                    "effective_coordinator": eff,
                    "coordinator_source": coord_source,
                    "current_attempt_id": task.get("current_attempt_id"),
                    "created_at": task.get("created_at"),
                    "updated_at": task.get("updated_at"),
                    "accepted_at": task.get("accepted_at"),
                    "closed_at": task.get("closed_at"),
                    "deadline_at": task.get("deadline_at"),
                    "next_check_at": task.get("next_check_at"),
                },
                "attempts": att_objs,
                "attempts_full_count": ledger.count_attempts_for_task(task_id),
                "attempts_truncated": ledger.count_attempts_for_task(task_id) > len(att_objs),
                "notifications": [{
                    "id": n.get("id"), "kind": n.get("kind"), "state": n.get("state"),
                    "target_session": n.get("target_session"),
                    "created_at": n.get("created_at"),
                    "summary": (n.get("payload") or {}).get("summary"),
                    "classification": (n.get("payload") or {}).get("classification"),
                } for n in notes],
            }
        finally:
            conn.close()

    def attempt_detail(self, attempt_id: str) -> dict:
        """尝试/证据详情（只读）：执行与投递事实、交付清单、验收结论。"""
        conn = connect(self.home)
        try:
            ledger = Ledger(conn, self.home)
            att = ledger.get_attempt(attempt_id)
            task = ledger.get_task(att["task_id"])
            dels = ledger.deliverables(attempt_id)
            return {
                "ok": True,
                "attempt": {
                    # 只回传尝试/证据详情页实际渲染所需字段；不暴露任意元数据或写范围。
                    "id": att["id"], "task_id": att["task_id"],
                    "generation": att["generation"], "session_id": att.get("session_id"),
                    "directory": att.get("directory"),
                    "status": att["status"], "dispatch_state": att.get("dispatch_state"),
                    "dispatch_msg_id": att.get("dispatch_msg_id"),
                    "created_at": att.get("created_at"),
                    "started_at": att.get("started_at"),
                    "dispatched_at": att.get("dispatched_at"),
                    "last_progress_at": att.get("last_progress_at"),
                    "last_observed_at": att.get("last_observed_at"),
                    "deadline_at": att.get("deadline_at"),
                    "stop_requested_at": att.get("stop_requested_at"),
                    "stop_confirmed_at": att.get("stop_confirmed_at"),
                    "site_released_at": att.get("site_released_at"),
                    "result_collected_at": att.get("result_collected_at"),
                    "result_path": att.get("result_path"),
                    "result_summary": (att.get("result_summary") or "")[:3000],
                    "terminal_at": att.get("terminal_at"),
                    "terminal_kind": att.get("terminal_kind"),
                    "terminal_reason": att.get("terminal_reason"),
                    "verification": att.get("verification") if isinstance(
                        att.get("verification"), dict) else {},
                },
                "task": {
                    "id": task["id"], "project": task.get("project"),
                    "title": task.get("title"), "status": task.get("status"),
                },
                "deliverables": [dict(x) for x in dels],
            }
        finally:
            conn.close()

    def unbound_tasks_page(self, *, after_id: str | None = None,
                           limit: int = 100) -> dict:
        """未绑定任务全量 keyset 分页（只读）。

        “未绑定”与总览语义一致：无显式 coordinator_session 且 project 无租约
        持有者；不受快照 200 项限制，无任意行数上限。
        """
        conn = connect(self.home)
        try:
            out = Ledger(conn, self.home).list_unbound_tasks_page(
                after_id=after_id, limit=limit)
        finally:
            conn.close()
        return {
            "ok": True,
            "tasks": [{
                "id": t["id"], "project": t.get("project"), "title": t.get("title"),
                "status": t.get("status"), "blocked_reason": t.get("blocked_reason"),
                "updated_at": t.get("updated_at"),
                "coordinator_session": t.get("coordinator_session"),
            } for t in out["rows"]],
            "full_count": out["full_count"],
            "next_after_id": out["next_after_id"],
            "truncated": out["truncated"],
            "scope": "未绑定任务 = 无显式 coordinator_session 且其 project 无项目租约持有者；"
                     "全量 keyset 分页（按 id 升序），不受快照 200 项限制",
        }

    # ---- S4 写入口（共享 ManagedInputs 用例；只解析参数，不复制业务/SQL）----
    def input_send(self, body: dict) -> dict:
        """execute/park/record 显式意图；task/tracking 参数精确透传。"""
        session = str(body.get("session") or "")
        intent = str(body.get("intent") or "")
        text = body.get("text")
        if not isinstance(text, str) or not text.strip():
            raise ActionError("usage", "input send 需要非空 text",
                              {"session_id": session})
        owner = str(body.get("owner") or "")
        request_id = str(body.get("request_id") or "")
        if not owner or not request_id:
            raise ActionError("usage", "input send 需要 owner 与 request_id",
                              {"session_id": session})
        kw: dict = {"intent": intent, "text": text, "owner": owner,
                    "request_id": request_id,
                    "project": str(body.get("project") or "")}
        expires_at = body.get("expires_at")
        if expires_at is not None:
            kw["expires_at"] = int(expires_at)
        if body.get("flush_condition") is not None:
            kw["flush_condition"] = str(body["flush_condition"])
        for key, bkey in (("task_id", "task"), ("attempt_id", "attempt"),
                          ("target_session", "target_session"),
                          ("work_kind", "work_kind")):
            val = body.get(bkey)
            if val not in (None, ""):
                if bkey in ("task", "attempt", "target_session", "work_kind"):
                    kw[key] = str(val)
        deadline = body.get("deadline")
        if deadline is not None:
            kw["deadline"] = int(deadline)
        materials = body.get("materials")
        if isinstance(materials, list) and materials:
            kw["materials"] = materials
        if body.get("untracked") is True:
            kw["untracked"] = True
        with _open_managed_inputs(self.home,
                                  client_factory=self.client_factory) as mi:
            out = mi.send(session, **kw)
        return {"ok": True, "out": out}

    def input_flush(self, input_id: str, body: dict) -> dict:
        return self._input_mutation("flush", input_id, body)

    def input_cancel(self, input_id: str, body: dict) -> dict:
        return self._input_mutation("cancel", input_id, body)

    def input_ack(self, input_id: str, body: dict) -> dict:
        value = body.get("value")
        if not isinstance(value, bool):
            raise ActionError("usage",
                              "ack 必须提供显式 bool value；false 不构成 ACK",
                              {"input_id": input_id})
        owner = str(body.get("owner") or "")
        confirm = str(body.get("confirm") or "")
        expected = body.get("expected_revision")
        reason = str(body.get("reason") or "")
        if not owner or not reason or not isinstance(expected, int) or expected < 1:
            raise ActionError("usage",
                              "ack 需要 owner/confirm/expected_revision/reason",
                              {"input_id": input_id})
        with _open_managed_inputs(self.home,
                                  client_factory=self.client_factory) as mi:
            out = mi.acknowledge(input_id, value=value, owner=owner,
                                 confirm=confirm,
                                 expected_revision=expected, reason=reason)
        return {"ok": True, "out": out}

    def _input_mutation(self, action: str, input_id: str, body: dict) -> dict:
        owner = str(body.get("owner") or "")
        confirm = str(body.get("confirm") or "")
        expected = body.get("expected_revision")
        reason = str(body.get("reason") or "")
        if not owner or not reason or not isinstance(expected, int) or expected < 1:
            raise ActionError("usage",
                              f"{action} 需要 owner/confirm/expected_revision/reason",
                              {"input_id": input_id})
        with _open_managed_inputs(self.home,
                                  client_factory=self.client_factory) as mi:
            out = getattr(mi, action)(input_id, owner=owner, confirm=confirm,
                                      expected_revision=expected, reason=reason)
        return {"ok": True, "out": out}

    def run_supervisor_cli(self, *args: str) -> dict:
        """Supervisor 生命周期唯一允许的子进程路径（支持 wheel：-m oc_orch.cli）。"""
        env = os.environ.copy()
        env["OC_ORCH_HOME"] = self.home
        root = _module_root()
        if root:
            env["PYTHONPATH"] = root + (os.pathsep + env["PYTHONPATH"]
                                        if env.get("PYTHONPATH") else "")
        cmd = [sys.executable, "-m", "oc_orch.cli", "--home", self.home,
               "--json", *args]
        p = subprocess.run(cmd, text=True, capture_output=True,
                           timeout=SUPERVISOR_TIMEOUT, env=env)
        try:
            out = json.loads(p.stdout) if p.stdout.strip() else {}
        except ValueError:
            out = {"raw": p.stdout}
        if p.returncode != 0:
            code = out.get("code") or f"EXIT_{p.returncode}"
            msg = out.get("message") or p.stderr.strip()[:800] or "CLI failed"
            raise WebActionError(code, msg, HTTPStatus.BAD_REQUEST, out)
        return out

    def start(self) -> int:
        server = self

        class Handler(RequestHandler):
            web = server

        self.httpd = ThreadingHTTPServer((self.host, self.port), Handler)
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       name="oc-orch-web", daemon=True)
        self.thread.start()
        atomic_write(os.path.join(self.home, PID_NAME), json_dumps({
            "pid": os.getpid(), "host": self.host, "port": self.port,
            "started_at": now_ms(), "read_only": self.read_only,
        }), 0o600)
        return self.port

    def serve_forever(self) -> None:
        if self.httpd is None:
            self.start()
        assert self.httpd is not None
        self.httpd.serve_forever()

    def stop(self) -> None:
        if self.httpd is not None:
            self.httpd.shutdown()
            self.httpd.server_close()
            self.httpd = None
        try:
            os.unlink(os.path.join(self.home, PID_NAME))
        except OSError:
            pass


class WebActionError(Exception):
    def __init__(self, code: str, message: str, status: int = HTTPStatus.BAD_REQUEST,
                 detail=None):
        self.code = code
        self.message = message
        self.status = status
        self.detail = detail
        super().__init__(message)


class RequestHandler(BaseHTTPRequestHandler):
    web: WebServer
    server_version = f"oc-orch-web/{__version__}"

    # ---- 基础响应 -------------------------------------------------
    def log_message(self, fmt, *args):
        if self.web.verbose:
            sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _host_ok(self) -> bool:
        host = (self.headers.get("Host") or "").strip()
        name = host.split(":")[0].strip("[]").lower()
        return name in ("127.0.0.1", "localhost", "::1")

    def _authed(self) -> bool:
        # 本期无 token：仅依赖 loopback 绑定 + Host/Origin/CSRF 限制。
        return True

    def _origin_ok(self) -> bool:
        origin = (self.headers.get("Origin") or "").strip()
        if not origin:
            return True  # 非浏览器客户端；本机可信
        parsed = urllib.parse.urlsplit(origin)
        if parsed.scheme not in ("http", "https"):
            return False
        host = (parsed.hostname or "").lower()
        return host in ("127.0.0.1", "localhost", "::1")

    def _send(self, status: int, body: bytes, ctype: str = "application/json",
              headers: dict | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, obj: dict) -> None:
        self._send(status, json.dumps(obj, ensure_ascii=False).encode("utf-8"))

    def _error(self, status: int, code: str, message: str, detail=None) -> None:
        self._json(status, {"ok": False, "code": code, "message": message,
                            "detail": detail})

    def _body(self) -> dict:
        ctype = (self.headers.get("Content-Type") or "").split(";")[0].strip()
        if ctype != "application/json":
            raise WebActionError("BAD_CONTENT_TYPE", "需要 application/json",
                                 HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_BODY:
            raise WebActionError("BODY_TOO_LARGE", "请求体过大",
                                 HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            data = json.loads(raw)
        except ValueError:
            raise WebActionError("BAD_JSON", "JSON 解析失败")
        if not isinstance(data, dict):
            raise WebActionError("BAD_JSON", "请求体必须是对象")
        return data

    # ---- GET ------------------------------------------------------
    def do_GET(self) -> None:
        if not self._host_ok():
            return self._error(HTTPStatus.FORBIDDEN, "BAD_HOST", "Host 不被允许")
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/":
            return self._send(HTTPStatus.OK, HTML_PAGE.encode("utf-8"),
                              "text/html; charset=utf-8")
        if parsed.path == "/favicon.ico":
            return self._send(HTTPStatus.NO_CONTENT, b"", "image/x-icon")
        # ---- 同源静态资源：固定白名单，严禁任意路径遍历/暴露源码与锁文件 ----
        # /assets/* 只允许 HTTP_WHITELIST 内相对文件名；解码后再校验，目录穿越
        # （含 %2e%2e / %2f 变体）一律 404，不提示包内布局。
        if parsed.path.startswith("/assets/"):
            name = urllib.parse.unquote(parsed.path[len("/assets/"):])
            if name not in HTTP_WHITELIST:
                return self._error(HTTPStatus.NOT_FOUND, "NOT_FOUND", "资源不存在")
            try:
                return self._send(HTTPStatus.OK, read_bytes(name),
                                  content_type(name))
            except (FileNotFoundError, ValueError) as e:
                return self._error(HTTPStatus.NOT_FOUND, "NOT_FOUND", str(e))
        if not self._authed():
            return self._error(HTTPStatus.UNAUTHORIZED, "UNAUTHORIZED", "需要登录")
        if parsed.path == "/api/status":
            try:
                return self._json(HTTPStatus.OK, {"ok": True,
                                                  "snapshot": self.web.snapshot()})
            except Exception as e:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "SNAPSHOT_FAILED",
                                   repr(e))
        if parsed.path == "/api/coordinator-titles":
            try:
                return self._json(HTTPStatus.OK, {
                    "ok": True, "titles": self.web.coordinator_titles()
                })
            except Exception as e:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "TITLES_FAILED",
                                   repr(e))
        if parsed.path == "/api/audit":
            q = urllib.parse.parse_qs(parsed.query)
            limit = 50
            try:
                limit = int((q.get("limit") or ["50"])[0])
            except ValueError:
                pass
            return self._json(HTTPStatus.OK, {"ok": True,
                                              "audit": self.web.audit(limit)})
        if parsed.path == "/api/inputs":
            q = urllib.parse.parse_qs(parsed.query)
            session = (q.get("session") or [None])[0]
            limit = _clamp_limit((q.get("limit") or ["50"])[0])
            try:
                return self._json(HTTPStatus.OK, self.web.managed_inputs(
                    session=session, limit=limit))
            except Exception as e:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "INPUTS_FAILED",
                                   repr(e))
        if parsed.path == "/api/input-watches":
            q = urllib.parse.parse_qs(parsed.query)
            limit = _clamp_limit((q.get("limit") or ["50"])[0])
            try:
                return self._json(HTTPStatus.OK, self.web.managed_watches(limit=limit))
            except Exception as e:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "WATCHES_FAILED",
                                   repr(e))
        if parsed.path == "/api/responsibilities":
            q = urllib.parse.parse_qs(parsed.query)
            task_id = (q.get("task") or [None])[0]
            owner = (q.get("owner") or [None])[0]
            limit = _clamp_limit((q.get("limit") or ["100"])[0])
            try:
                return self._json(HTTPStatus.OK, self.web.responsibilities(
                    task_id=task_id, owner=owner, limit=limit))
            except Exception as e:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR,
                                   "RESPONSIBILITIES_FAILED", repr(e))
        if parsed.path == "/api/health":
            # keep_blank_values=True：空 scope 值必须能被共享用例识别为 INVALID_SCOPE
            q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            project = (q.get("project") or [None])[0]
            session_id = (q.get("session_id") or [None])[0]
            after_id = (q.get("after_id") or [None])[0]
            limit_raw = (q.get("limit") or [None])[0]
            limit: object = 50
            if limit_raw is not None:
                try:
                    limit = int(limit_raw)
                except ValueError:
                    limit = limit_raw  # 交给共享用例稳定 INVALID_LIMIT → 400
            try:
                return self._json(HTTPStatus.OK, self.web.continuity_health(
                    project=project, session_id=session_id, limit=limit,
                    after_id=after_id))
            except LedgerError as e:
                return self._error(HTTPStatus.BAD_REQUEST,
                                   e.code or "INVALID_SCOPE", str(e))
            except Exception as e:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR,
                                    "HEALTH_FAILED", repr(e))
        if parsed.path.startswith("/api/prototype/"):
            task_id = urllib.parse.unquote(parsed.path[len("/api/prototype/"):])
            q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            if not task_id or "/" in task_id or set(q) - {"operation_id"} or any(len(v) != 1 for v in q.values()):
                return self._error(HTTPStatus.BAD_REQUEST, "PROTOTYPE_INVALID", "须精确Task与可选原operation_id")
            try:
                return self._json(HTTPStatus.OK, self.web.prototype(task_id, operation_id=q.get("operation_id", [None])[0]))
            except LedgerError as e:
                return self._error(HTTPStatus.BAD_REQUEST, e.code, str(e))
        if parsed.path == "/api/coordinator/message":
            q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            if set(q) != {"project", "session_id", "operation_id"} or any(len(v) != 1 for v in q.values()):
                return self._error(HTTPStatus.BAD_REQUEST, "OPERATOR_TURN_INVALID", "须精确指定原操作")
            try:
                return self._json(HTTPStatus.OK, self.web.operator_turn("show", {k: v[0] for k, v in q.items()}))
            except LedgerError as e:
                return self._error(HTTPStatus.BAD_REQUEST, e.code, str(e))
        if parsed.path == "/api/coordinator/turn-candidate":
            q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            if set(q) != {"project", "session_id"} or any(
                    len(v) != 1 for v in q.values()):
                return self._error(HTTPStatus.BAD_REQUEST,
                                   "TURN_CANDIDATE_INVALID",
                                   "只接受单一 project/session_id")
            try:
                return self._json(HTTPStatus.OK, self.web.turn_candidate(
                    project=q["project"][0],
                    session_id=q["session_id"][0]))
            except LedgerError as e:
                return self._error(HTTPStatus.BAD_REQUEST, e.code, str(e))
            except Exception:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR,
                                   "TURN_CANDIDATE_FAILED",
                                   "候选回合不可读，拒绝推断")
        if parsed.path == "/api/coordinator/control":
            q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            if (set(q) - {"project", "session_id", "operation_id"}
                    or any(len(v) != 1 for v in q.values())):
                return self._error(HTTPStatus.BAD_REQUEST, "INVALID_SCOPE", "只接受单一显式作用域")
            try:
                return self._json(HTTPStatus.OK, self.web.project_control_view(
                    project=(q.get("project") or [None])[0],
                    session_id=(q.get("session_id") or [None])[0],
                    operation_id=(q.get("operation_id") or [None])[0]))
            except LedgerError as e:
                return self._error(HTTPStatus.BAD_REQUEST, e.code, str(e))
            except Exception:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "CONTROL_VIEW_FAILED", "控制状态不可读，拒绝推断")
        if parsed.path == "/api/delivery-policy":
            q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            if set(q) - {"task_id", "project"} or any(len(v) != 1 for v in q.values()):
                return self._error(HTTPStatus.BAD_REQUEST, "DELIVERY_POLICY_INVALID", "只接受单一显式作用域")
            try:
                return self._json(HTTPStatus.OK, self.web.delivery_policy(
                    task_id=(q.get("task_id") or [None])[0], project=(q.get("project") or [None])[0]))
            except LedgerError as e:
                return self._error(HTTPStatus.BAD_REQUEST, e.code, str(e))
        if parsed.path == "/api/incidents":
            q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            project = (q.get("project") or [None])[0]
            after_id = (q.get("after_id") or [None])[0]
            try:
                limit = int((q.get("limit") or ["50"])[0])
                return self._json(HTTPStatus.OK, self.web.incidents(
                    project=project, limit=limit, after_id=after_id))
            except (ValueError, LedgerError):
                return self._error(HTTPStatus.BAD_REQUEST, "INVALID_SCOPE",
                                   "incident scope/limit/after_id 非法")
            except Exception as e:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR,
                                   "INCIDENTS_FAILED", repr(e))
        if parsed.path == "/api/coordinators":
            try:
                return self._json(HTTPStatus.OK, self.web.coordinator_overview())
            except Exception as e:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR,
                                   "COORDINATORS_FAILED", repr(e))
        # ---- 未绑定任务全量分页（只读；复数前缀，与 /api/task/<id> 区分）----
        if parsed.path.startswith("/api/tasks/unbound"):
            q = urllib.parse.parse_qs(parsed.query)
            after_id = (q.get("after_id") or [None])[0]
            limit = _clamp_limit((q.get("limit") or ["100"])[0])
            try:
                return self._json(HTTPStatus.OK, self.web.unbound_tasks_page(
                    after_id=after_id, limit=limit))
            except Exception as e:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR,
                                   "UNBOUND_TASKS_FAILED", repr(e))
        # ---- 调度者入口深链只读 API（新增；保持既有 JSON/安全契约）----
        if parsed.path.startswith("/api/coordinator/"):
            sid = urllib.parse.unquote(parsed.path[len("/api/coordinator/"):])
            if not sid or "/" in sid:
                return self._error(HTTPStatus.NOT_FOUND, "NOT_FOUND", "未知路径")
            q = urllib.parse.parse_qs(parsed.query)
            after_id = (q.get("after_id") or [None])[0]
            limit = _clamp_limit((q.get("limit") or ["200"])[0])
            try:
                return self._json(HTTPStatus.OK, self.web.coordinator_detail(
                    sid, after_id=after_id, limit=limit))
            except LedgerError as e:
                return self._error(HTTPStatus.NOT_FOUND, e.code or "NOT_FOUND",
                                   str(e))
            except Exception as ex:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR,
                                   "COORDINATOR_DETAIL_FAILED", repr(ex))
        if parsed.path.startswith("/api/task/"):
            tid = urllib.parse.unquote(parsed.path[len("/api/task/"):])
            if not tid or "/" in tid:
                return self._error(HTTPStatus.NOT_FOUND, "NOT_FOUND", "未知路径")
            try:
                return self._json(HTTPStatus.OK, self.web.task_detail(tid))
            except LedgerError as e:
                return self._error(HTTPStatus.NOT_FOUND, e.code or "NOT_FOUND",
                                   str(e))
            except Exception as ex:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR,
                                   "TASK_DETAIL_FAILED", repr(ex))
        if parsed.path.startswith("/api/attempt/"):
            aid = urllib.parse.unquote(parsed.path[len("/api/attempt/"):])
            if not aid or "/" in aid:
                return self._error(HTTPStatus.NOT_FOUND, "NOT_FOUND", "未知路径")
            try:
                return self._json(HTTPStatus.OK, self.web.attempt_detail(aid))
            except LedgerError as e:
                return self._error(HTTPStatus.NOT_FOUND, e.code or "NOT_FOUND",
                                   str(e))
            except Exception as ex:
                return self._error(HTTPStatus.INTERNAL_SERVER_ERROR,
                                   "ATTEMPT_DETAIL_FAILED", repr(ex))
        return self._error(HTTPStatus.NOT_FOUND, "NOT_FOUND", "未知路径")

    # ---- POST -----------------------------------------------------
    def do_POST(self) -> None:
        if not self._host_ok():
            return self._error(HTTPStatus.FORBIDDEN, "BAD_HOST", "Host 不被允许")
        if (self.headers.get("X-OC-Orch-Web") != "1"):
            return self._error(HTTPStatus.FORBIDDEN, "CSRF", "缺少 X-OC-Orch-Web")
        if not self._origin_ok():
            return self._error(HTTPStatus.FORBIDDEN, "BAD_ORIGIN", "Origin 不被允许")
        parsed = urllib.parse.urlsplit(self.path)
        try:
            body = self._body()
        except WebActionError as e:
            return self._error(e.status, e.code, e.message)
        if not self._authed():
            return self._error(HTTPStatus.UNAUTHORIZED, "UNAUTHORIZED", "需要登录")
        if self.web.read_only:
            return self._error(HTTPStatus.FORBIDDEN, "READ_ONLY", "只读模式")
        try:
            return self._action(parsed.path, body)
        except WebActionError as e:
            return self._error(e.status, e.code, e.message, e.detail)
        except subprocess.TimeoutExpired:
            return self._error(HTTPStatus.GATEWAY_TIMEOUT, "TIMEOUT",
                               "Supervisor 操作超时")
        except Exception as e:
            return self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "ACTION_FAILED",
                               repr(e))

    # ---- 动作 -----------------------------------------------------
    def _action(self, path: str, body: dict):
        """每个请求独立装配应用与连接；写动作不经过任何子进程。"""
        parts = [p for p in path.split("/") if p]
        try:
            with open_application(self.web.home,
                                  client_factory=self.web.client_factory) as app:
                return self._handle(parts, body, app)
        except ActionError as e:
            # 维持原 CLI 错误嵌套形状：error.detail = {"ok":False, code, message, detail}
            raise WebActionError(e.code, e.message, HTTPStatus.BAD_REQUEST,
                                 {"ok": False, "code": e.code,
                                  "message": e.message, "detail": e.detail})
        except LedgerError as e:
            raise WebActionError(
                e.code, str(e), HTTPStatus.BAD_REQUEST,
                {"ok": False, "code": e.code, "message": str(e), "detail": e.detail})
        except TargetGateStateError as e:
            raise WebActionError(
                "gate", f"闸门状态损坏，拒绝解锁：{e}", HTTPStatus.BAD_REQUEST)
        except (ApiError, TransportError, OrchError) as e:
            raise WebActionError("service", str(e), HTTPStatus.BAD_REQUEST,
                                 {"ok": False, "code": "service",
                                  "message": str(e), "detail": None})

    def _handle(self, parts: list[str], body: dict, app):
        if len(parts) == 3 and parts[:2] == ["api", "prototype"]:
            return self._json(HTTPStatus.OK, self.web.prototype(parts[2], body))
        if parts == ["api", "coordinator", "message"]:
            return self._json(HTTPStatus.OK, self.web.operator_turn("send", body))
        if parts == ["api", "coordinator", "message", "observe"]:
            return self._json(HTTPStatus.OK, self.web.operator_turn("observe", body))
        if len(parts) == 3 and parts[:2] == ["api", "delivery-policy"] and parts[2] in ("task", "project"):
            return self._json(HTTPStatus.OK, self.web.delivery_policy_apply(parts[2], body))
        # /api/input/send 与 /api/input/<id>/flush|cancel|ack（共享 ManagedInputs）
        if len(parts) == 3 and parts[:2] == ["api", "input"] and parts[2] == "send":
            return self._json(HTTPStatus.OK, self.web.input_send(body))
        if len(parts) == 4 and parts[:2] == ["api", "input"] \
                and parts[3] in ("flush", "cancel", "ack"):
            input_id = parts[2]
            if parts[3] == "flush":
                out = self.web.input_flush(input_id, body)
            elif parts[3] == "cancel":
                out = self.web.input_cancel(input_id, body)
            else:
                out = self.web.input_ack(input_id, body)
            return self._json(HTTPStatus.OK, out)
        # /api/notify/ack  {"ids":[...]}
        if parts == ["api", "notify", "ack"]:
            ids = [str(x) for x in (body.get("ids") or [])]
            if not ids:
                raise WebActionError("BAD_REQUEST", "缺少 ids")
            return self._json(HTTPStatus.OK, {"ok": True,
                                              "result": app.ack_notifications(ids)})
        if parts == ["api", "alerts", "ack"]:
            ids = [str(x) for x in (body.get("ids") or [])]
            if not ids:
                raise WebActionError("BAD_REQUEST", "缺少 ids")
            return self._json(HTTPStatus.OK, {"ok": True,
                                              "result": app.ack_alerts(ids)})
        if len(parts) == 4 and parts[:2] == ["api", "task"] \
                and parts[3] == "rework":
            task_id = parts[2]
            if body.get("confirm") != task_id:
                raise WebActionError(
                    "CONFIRM_REQUIRED", f"返工需输入任务 ID 确认：{task_id}")
            feedback = body.get("feedback")
            if not isinstance(feedback, str) or not feedback.strip():
                raise WebActionError("BAD_REQUEST", "返工反馈不能为空")
            result = app.rework(
                task_id, feedback=feedback,
                session=(str(body["session"]) if body.get("session") else None),
                work_mode=str(body.get("work_mode") or "latency-first"))
            return self._json(HTTPStatus.OK, {"ok": True, "result": result})
        if len(parts) == 4 and parts[:2] == ["api", "task"] and parts[3] in (
                "pause", "resume", "cancel"):
            task_id, action = parts[2], parts[3]
            if action == "cancel":
                if body.get("confirm") != task_id:
                    raise WebActionError(
                        "CONFIRM_REQUIRED", f"取消需输入任务 ID 确认：{task_id}")
                result = app.cancel(task_id, reason=str(body.get("reason")
                                                        or "web-cancel"),
                                    interrupt=bool(body.get("interrupt")))
            elif action == "pause":
                result = app.pause(task_id, reason=str(body.get("reason")
                                                       or "web-pause"))
            else:
                result = app.resume(task_id)
            return self._json(HTTPStatus.OK, {"ok": True, "result": result})
        if len(parts) == 4 and parts[:2] == ["api", "attempt"] and parts[3] == "collect":
            return self._json(HTTPStatus.OK, {"ok": True,
                                              "result": app.collect(parts[2])})
        if len(parts) == 4 and parts[:2] == ["api", "attempt"] and parts[3] == "verify":
            attempt_id = parts[2]
            passed = bool(body.get("pass"))
            confirm = str(body.get("confirm") or "")
            if confirm != attempt_id:
                raise WebActionError(
                    "CONFIRM_REQUIRED", f"验收需输入尝试 ID 确认：{attempt_id}")
            result = app.verify(
                attempt_id, passed=passed, reason=str(body.get("reason") or ""),
                evidence=[str(x) for x in (body.get("evidence") or [])],
                notes=str(body.get("notes") or ""),
                force=bool(body.get("force")),
                run_checks=bool(body.get("run_checks")),
                check_timeout=120, actor="coordinator",
            )
            return self._json(HTTPStatus.OK, {"ok": True, "result": result})
        if len(parts) == 3 and parts[:2] == ["api", "coordinator"] \
                and parts[2] in ("pause", "resume"):
            return self._json(HTTPStatus.OK,
                              self.web.coordinator_control(parts[2], body))
        if parts == ["api", "coordinator", "release-hold"]:
            return self._json(HTTPStatus.OK, self.web.release_hold(body))
        if parts == ["api", "coordinator", "register-turn"]:
            return self._json(HTTPStatus.OK,
                              self.web.register_completed_turn(body))
        if parts == ["api", "coordinator", "unlock"]:
            project = str(body.get("project") or "").strip()
            session = str(body.get("session") or "").strip() or None
            reason = str(body.get("reason") or "").strip() or "web-manual-unlock"
            allow_provider = (body.get("allow_provider") is True
                              or str(body.get("allow_provider")
                                     or "").strip().lower() == "true")
            if not project:
                raise WebActionError("BAD_REQUEST", "缺少 project")
            if allow_provider and not str(body.get("reason") or "").strip():
                raise WebActionError(
                    "BAD_REQUEST", "解锁 provider 闸门必须提供原因（operator 动作）")
            if body.get("confirm") != "UNLOCK":
                raise WebActionError(
                    "CONFIRM_REQUIRED", "需输入 UNLOCK 确认解锁闸门")
            gate = make_target_gate(app.ledger.conn, app.clock)
            result = unlock_manual_gate(
                app.ledger, gate, project=project, session=session,
                reason=reason, actor="web", now=now_ms(),
                allow_provider=allow_provider)
            return self._json(HTTPStatus.OK, {"ok": True, "result": result})
        if parts == ["api", "supervisor", "start"]:
            if body.get("confirm") != "START":
                raise WebActionError("CONFIRM_REQUIRED", "需输入 START 确认")
            return self._json(HTTPStatus.OK, {"ok": True,
                                              "result": self.web.run_supervisor_cli(
                                                  "supervisor", "start")})
        if parts == ["api", "supervisor", "stop"]:
            if body.get("confirm") != "STOP":
                raise WebActionError("CONFIRM_REQUIRED", "需输入 STOP 确认")
            return self._json(HTTPStatus.OK, {"ok": True,
                                              "result": self.web.run_supervisor_cli(
                                                  "supervisor", "stop", "--force")})
        raise WebActionError("NOT_FOUND", "未知操作", HTTPStatus.NOT_FOUND)

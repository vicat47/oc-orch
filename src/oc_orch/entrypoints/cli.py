"""oc-orch 命令行：登记、派单、观察、收集、验收、恢复与守护。

业务用例经 application（composition.open_application）转调；入口只负责
参数/路径/stdin 边界与 JSON/退出码/human 输出。每命令独立连接，不改变
进程级环境变量。
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from typing import Any

from .. import __version__
from ..application.manual_recovery import unlock_manual_gate
from ..composition import (
    Client,
    Ledger,
    SystemClock,
    TargetGateStateError,
    init,
    load_config,
    load_service,
    make_target_gate,
)
from ..domain.errors import (
    ActionError,
    ApiError,
    LedgerError,
    OrchError,
    TransportError,
)
from ..domain.prompts import build_prompt  # noqa: F401 兼容门面：已迁移 domain/prompts.py
from ..runtime.supervisor import Supervisor, SupervisorBusy, supervisor_status
from ..util import (
    default_home,
    ensure_home,
    iso,
    json_dumps,
    json_loads,
    now_ms,
    short_id,
)

EXIT = {
    "ok": 0,
    "usage": 2,
    "service": 3,
    "site_busy": 4,
    "supervisor_busy": 5,
    "dispatch_unknown": 6,
    "gate": 7,
    "provider_backoff": 8,
}


def _open_application(home, *, client_factory=None):
    """延迟装配组合根；入口保持可独立导入。"""
    from ..composition import open_application

    return open_application(home, client_factory=client_factory)


def _open_managed_inputs(home, *, client_factory=None):
    """延迟装配受管输入组合根（composition.open_managed_inputs）。"""
    from ..composition import open_managed_inputs

    return open_managed_inputs(home, client_factory=client_factory)


def _open_responsibilities(home):
    """延迟装配后续责任组合根；入口只解析/展示，不写 SQL。"""
    from ..composition import open_responsibilities

    return open_responsibilities(home)


@contextmanager
def _open_coordinator_control(home):
    """显式对话控制入口：经父 composition factory 装配共享用例，独立连接。

    不推断真人来源、不访问服务；明确的本地 operator 控制动作由
    application.conversation_control 校验后转 CoordinatorControlService。
    """
    from ..composition import make_coordinator_control

    conn = init(ensure_home(home))
    try:
        yield make_coordinator_control(conn, SystemClock())
    finally:
        conn.close()


def _module_root() -> str | None:
    """子进程 `python -m oc_orch.cli` 的包源根。

    从已导入包动态计算：源码树开发与 wheel 安装均适用；不依赖 bin/目录布局。
    wheel 安装时指向 site-packages，前缀追加不改变解析。
    """
    import oc_orch  # noqa: F401

    return os.path.dirname(os.path.dirname(os.path.abspath(oc_orch.__file__)))


class Ctx:
    def __init__(self, args):
        self.args = args
        self.home = ensure_home(args.home)
        self.conn = init(self.home)
        self.ledger = Ledger(self.conn, self.home)
        self.cfg = load_config(self.home)
        self.client: Client | None = None

    def client_or_die(self) -> Client:
        if self.client is None:
            self.client = Client()
        return self.client

    @contextmanager
    def app(self, *, client_factory=None):
        """每个入口动作独立装配应用与连接；调用方作用域持有，结束即关。"""
        with _open_application(self.home, client_factory=client_factory or Client) as app:
            yield app

    @contextmanager
    def managed(self, *, client_factory=None):
        """每个受管输入动作独立装配组合根；调用方作用域持有，结束即关。"""
        with _open_managed_inputs(self.home,
                                  client_factory=client_factory or Client) as mi:
            yield mi

    @contextmanager
    def responsibilities(self):
        """后续责任只读/转交/关闭动作：共享用例经组合根装配。"""
        with _open_responsibilities(self.home) as store:
            yield store

    @contextmanager
    def coordinator_control(self):
        """每个显式对话控制动作独立装配共享用例；作用域结束即关。"""
        with _open_coordinator_control(self.home) as cc:
            yield cc


def emit(args, obj: Any, human: str | None = None) -> None:
    if args.json:
        print(json.dumps(obj, ensure_ascii=False, indent=1))
    elif human is not None:
        print(human)
    else:
        print(json.dumps(obj, ensure_ascii=False, indent=1))


def fail(args, code: str, message: str, detail: Any = None) -> int:
    if args.json:
        print(json.dumps({"ok": False, "code": code, "message": message,
                          "detail": detail}, ensure_ascii=False, indent=1))
    else:
        print(f"ERROR {code}: {message}", file=sys.stderr)
        if detail:
            print(json.dumps(detail, ensure_ascii=False, indent=1), file=sys.stderr)
    # carry 的稳定业务原因码保留在 JSON，输入错误沿用既有 usage 退出类别。
    return EXIT["usage"] if code.startswith("CARRY_") else EXIT.get(code, 1)


def _task_human(t: dict) -> str:
    return (f"{t['id']} [{t['status']}] {t['title']} "
            f"attempt={t.get('current_attempt') or '-'} "
            f"session={t.get('session_id') or '-'} "
            f"reason={t.get('blocked_reason') or '-'}")


def _read_text_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _acceptance(args) -> list[str]:
    out = list(args.acceptance or [])
    if getattr(args, "acceptance_file", None):
        text = _read_text_file(args.acceptance_file)
        parsed = json_loads(text, None)
        if isinstance(parsed, list):
            out.extend(str(x) for x in parsed)
        else:
            out.extend(line.strip() for line in text.splitlines() if line.strip())
    return out


def _deliverables(args, task: dict | None = None) -> list[str]:
    out: list[str] = []
    if task:
        raw = (task.get("metadata") or {}).get("deliverables") or []
        for item in raw:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict) and item.get("target"):
                out.append(str(item["target"]))
    out.extend(getattr(args, "deliverable", None) or [])
    return list(dict.fromkeys(out))


def _checks(args, task: dict | None = None) -> list[str]:
    out: list[str] = []
    if task:
        out.extend((task.get("metadata") or {}).get("checks") or [])
    out.extend(getattr(args, "check", None) or [])
    return list(dict.fromkeys(out))


def _scope(args, task: dict | None = None, directory: str | None = None) -> list[str]:
    out = list(getattr(args, "scope", None) or [])
    if not out and task:
        out = list(task.get("write_scope") or [])
    if not out and directory:
        out = [directory]
    return out


def _coordinator_from(args) -> str | None:
    return (getattr(args, "coordinator", None)
            or os.environ.get("OC_ORCH_COORDINATOR_SESSION"))


#: 显式对话控制子命令 → application.conversation_control 方法名。
_CONTROL_METHOD = {
    "pause": "pause",
    "resume": "resume",
    "hold": "hold",
    "turn-begin": "begin_user_turn",
    "turn-finish": "finish_user_turn",
}


def _control_kwargs(args) -> dict:
    """把显式 CLI 参数组装为 conversation_control 结构化意图；无任何推断。"""
    if args.action in ("pause", "resume"):
        evidence = None
        raw_evidence = getattr(args, "evidence", None)
        if raw_evidence is not None and str(raw_evidence).strip():
            try:
                parsed = json.loads(raw_evidence)
            except ValueError:
                raise ActionError(
                    "usage", "--evidence 必须是合法 JSON 对象",
                    {"evidence": raw_evidence})
            if not isinstance(parsed, dict):
                raise ActionError(
                    "usage", "--evidence 必须是 JSON 对象（非数组/标量）",
                    {"evidence": raw_evidence})
            evidence = parsed
        return {
            "project": args.project, "session_id": args.session,
            "operation_id": args.operation_id, "reason": args.reason,
            "expected_revision": args.expected_revision,
            "expected_epoch": args.expected_epoch,
            "evidence": evidence,
        }
    if args.action == "hold":
        return {
            "session_id": args.session, "event_id": args.event_id,
            "origin_at": args.origin_at, "reason": args.reason,
        }
    if args.action == "turn-begin":
        return {
            "session_id": args.session, "message_id": args.message_id,
            "origin_at": args.origin_at, "operation_id": args.operation_id,
            "trusted": bool(args.trusted),
        }
    if args.action == "turn-finish":
        return {
            "session_id": args.session, "message_id": args.message_id,
            "idle_id": args.idle_id, "idle_at": args.idle_at,
            "expected_revision": args.expected_revision,
        }
    return {}


def _control_human(action: str, out: dict, args) -> str:
    """控制动作人类可读结果；只读展示不在这里冒充已执行。

    ``applied=False``（A 稳定 operation 历史幂等重放）时**不得**按动作名硬报
    paused/resumed：以 snapshot 的当前 status/revision 为准，明确「已处理但本次
    未变更」，避免旧操作重放被读成新的暂停/恢复。
    """
    result = out.get("result") or {}
    project = out.get("project")
    session = out.get("session_id")
    snap = result.get("snapshot") or {}
    status = snap.get("status")
    rev = result.get("revision")
    epoch = result.get("epoch")
    epoch_txt = str(epoch) if epoch is not None else "-"
    applied = result.get("applied") is True
    reason = result.get("reason")
    if action == "pause":
        if applied:
            return (f"paused project={project} session={session} "
                    f"revision={rev} epoch={epoch_txt} applied=yes\n"
                    "  本操作已应用：仅阻新自动派单/协调唤醒；不中断在途 "
                    "worker/任务/结果，不取消任务或释放写权")
        return (f"pause 已处理但本次未变更（历史 operation 幂等重放，applied=false）："
                f"status={status or '-'} revision={rev} epoch={epoch_txt}"
                f" reason={reason or '-'}\n"
                "  以 snapshot 当前状态为准，不再声称已暂停；未改写控制版本")
    if action == "resume":
        if applied:
            return (f"resumed project={project} session={session} "
                    f"revision={rev} epoch={epoch_txt} applied=yes\n"
                    "  本操作已应用：只恢复自动资格；不清 provider 冷却、Task "
                    "暂停/取消、回合 hold；不伪造 ACK/accepted，全部门禁通过后"
                    "才可自动发送")
        return (f"resume 已处理但本次未变更（历史 operation 幂等重放，applied=false）："
                f"status={status or '-'} revision={rev} epoch={epoch_txt}"
                f" reason={reason or '-'}\n"
                "  以 snapshot 当前状态为准，不再声称已恢复；未改写控制版本")
    if action == "hold":
        return (f"hold session={session} event={args.event_id} "
                f"origin_at={args.origin_at} reason={args.reason}\n"
                "  回合级 hold：不取消项目/worker；超时与自动 wake 不自行解除")
    if action == "turn-begin":
        return (f"user-turn begin session={session} message={args.message_id} "
                f"trusted=yes operation={args.operation_id}\n"
                "  回合屏障已建立（显式可信判定）；本轮结束后须用可信结束证据"
                "显式 finish-user-turn；native hook 未接通")
    if action == "turn-finish":
        return (f"user-turn finish session={session} message={args.message_id} "
                f"idle={args.idle_id} idle_at={args.idle_at} "
                f"revision={args.expected_revision}\n"
                "  本地 operator 断言已登记（原输入关联 + CAS）；真实执行归属"
                "由 runtime/台账证据核验，本命令不查询上游；空 inbox/"
                "session.updated 不是完成依据")
    return json.dumps(out, ensure_ascii=False, indent=1)


def _control_status_human(out: dict) -> str:
    """control 只读状态渲染：字段缺失不猜、不冒充已执行。"""
    lines = ["control"]
    proj = out.get("project")
    if proj:
        pause = proj.get("pause") or {}
        lines.append(f"  project={proj.get('project') or proj.get('id') or '?'} "
                     f"holder={proj.get('holder_session') or '-'} "
                     f"epoch={proj.get('epoch') or '-'} "
                     f"revision={proj.get('revision') or '-'} "
                     f"paused={'yes' if proj.get('paused') else 'no'}")
        if pause:
            at = (f" requested_at={pause.get('requested_at')}"
                  if pause.get("requested_at") is not None else "")
            lines.append(f"    pause: op={pause.get('operation_id') or '-'} "
                         f"reason={pause.get('reason') or '-'} "
                         f"requested_by={pause.get('requested_by') or '-'}{at}")
    sess = out.get("session")
    if sess:
        projects = [p for p in (sess.get("projects") or []) if p]
        lines.append(f"  session={sess.get('session_id') or sess.get('id') or '?'} "
                     f"projects={','.join(projects) if projects else '-'}")
        hold = sess.get("hold")
        if hold:
            at = (f" origin_at={hold.get('origin_at')}"
                  if hold.get("origin_at") is not None else "")
            lines.append(f"    hold: event={hold.get('event_id') or '-'} "
                         f"reason={hold.get('reason') or '-'}{at}")
        turn = sess.get("turn")
        if turn:
            lines.append(f"    turn: message={turn.get('message_id') or '-'} "
                         f"active={'yes' if turn.get('active') else 'no'} "
                         f"trusted={'yes' if turn.get('trusted') else 'no'} "
                         f"op={turn.get('operation_id') or '-'}")
    permits = out.get("permits")
    if permits is not None:
        blocking = permits.get("blocking") or []
        extra = (f" ({', '.join(str(b) for b in blocking)})" if blocking else "")
        lines.append(f"  permits={'yes' if permits.get('permits') else 'no'}{extra}")
    lines.append("  只读展示；控制版本/代次以 service 返回为准，不是已执行证明")
    return "\n".join(lines)


# ---- 命令实现 -----------------------------------------------------
def cmd_init(args) -> int:
    ctx = Ctx(args)
    emit(args, {
        "ok": True, "home": ctx.home, "db": os.path.join(ctx.home, "ledger.db"),
        "config": ctx.cfg, "schema_version": ctx.ledger.get_meta("schema_version"),
    }, f"ok home={ctx.home}\n db={os.path.join(ctx.home, 'ledger.db')}")
    return 0


def cmd_service(args) -> int:
    svc = load_service()
    client = Client(service=svc)
    info = None
    err = None
    try:
        info = client.info()
    except Exception as e:
        err = repr(e)
    obj = {"ok": err is None, "service": svc.public(), "info": info, "error": err}
    emit(args, obj, json.dumps(obj, ensure_ascii=False, indent=1))
    return 0 if err is None else EXIT["service"]


def cmd_doctor(args) -> int:
    ctx = Ctx(args)
    checks: dict[str, Any] = {}
    checks["home"] = {"ok": os.path.isdir(ctx.home), "path": ctx.home,
                      "mode": oct(os.stat(ctx.home).st_mode & 0o777)}
    try:
        checks["db"] = {"ok": True, "path": os.path.join(ctx.home, "ledger.db"),
                        "counts": ctx.ledger.counts(),
                        "schema_version": ctx.ledger.get_meta("schema_version")}
    except Exception as e:
        checks["db"] = {"ok": False, "error": repr(e)}
    try:
        client = ctx.client_or_die()
        info = client.info()
        checks["service"] = {"ok": True, "public": client.service.public(), "info": info}
    except Exception as e:
        checks["service"] = {"ok": False, "error": repr(e)}
    checks["supervisor"] = supervisor_status(ctx.home, ctx.cfg)
    checks["supervisor"]["ok"] = True  # Supervisor 是可选常驻组件；健康状态见 healthy
    checks["supervisor"]["running"] = checks["supervisor"]["healthy"]
    if args.probe_events:
        try:
            client = ctx.client_or_die()
            got = False
            for ev in client.iter_events(["session.execution.succeeded"],
                                         socket_timeout=5):
                if ev.get("type") == "server.connected":
                    got = True
                    break
            checks["event_probe"] = {"ok": got, "connected": got}
        except Exception as e:
            checks["event_probe"] = {"ok": False, "error": repr(e)}
    ok = all(v.get("ok", True) for v in checks.values() if isinstance(v, dict))
    emit(args, {"ok": ok, "checks": checks},
         "\n".join(f"{k}: {'OK' if v.get('ok') else 'FAIL'} {json_dumps(v)[:300]}"
                   for k, v in checks.items()))
    return 0 if ok else 1


def cmd_task_create(args) -> int:
    ctx = Ctx(args)
    task = ctx.ledger.create_task(
        project=args.project, title=args.title, objective=args.objective or "",
        acceptance=_acceptance(args), write_scope=_scope(args),
        coordinator_session=args.coordinator, priority=args.priority,
        task_id=args.task_id, metadata={
            "deliverables": _deliverables(args),
            "checks": _checks(args),
            **(json_loads(args.metadata, {}) if args.metadata else {}),
        },
    )
    emit(args, {"ok": True, "task": task}, _task_human({
        **task, "current_attempt": task.get("current_attempt_id"),
        "session_id": None,
    }))
    return 0


def cmd_task_list(args) -> int:
    ctx = Ctx(args)
    tasks = ctx.ledger.list_tasks(status=args.status, project=args.project)
    attempts = {a["id"]: a for a in ctx.ledger.list_attempts(limit=500)}
    rows = []
    for t in tasks:
        cur = attempts.get(t.get("current_attempt_id"))
        rows.append({**t, "current_attempt": cur["id"] if cur else None,
                     "attempt_status": cur["status"] if cur else None,
                     "session_id": cur.get("session_id") if cur else None})
    emit(args, {"ok": True, "tasks": rows},
         "\n".join(_task_human(r) for r in rows) or "(no tasks)")
    return 0


def cmd_task_show(args) -> int:
    ctx = Ctx(args)
    task = ctx.ledger.get_task(args.task)
    atts = ctx.ledger.list_attempts(task_id=args.task, limit=100)
    obj = {"ok": True, "task": task, "attempts": atts}
    lines = [_task_human({**task, "current_attempt": task.get("current_attempt_id"),
                          "session_id": atts[0].get("session_id") if atts else None})]
    for a in reversed(atts):
        lines.append(f"  {a['id']} g{a['generation']} [{a['status']}] "
                     f"session={a.get('session_id')} result={a.get('result_path') or '-'}")
    emit(args, obj, "\n".join(lines))
    return 0


def cmd_task_pause(args) -> int:
    ctx = Ctx(args)
    task = ctx.ledger.get_task(args.task)
    already = task["status"] == "paused"
    with ctx.app() as app:
        out = app.pause(args.task, reason=args.reason or "manual-pause")
    if already:
        emit(args, out, f"{args.task} already paused")
    else:
        from_ = (out["task"]["metadata"] or {}).get("paused_from")
        emit(args, out, f"{args.task} paused (from {from_})")
    return 0


def cmd_task_resume(args) -> int:
    ctx = Ctx(args)
    with ctx.app() as app:
        out = app.resume(args.task)
    emit(args, out, f"{args.task} resumed -> {out['task']['status']}")
    return 0


def cmd_task_cancel(args) -> int:
    ctx = Ctx(args)
    task = ctx.ledger.get_task(args.task)
    already = task["status"] == "cancelled"
    with ctx.app() as app:
        out = app.cancel(args.task, reason=args.reason, interrupt=args.interrupt)
    emit(args, out, f"{args.task} cancelled" if not already else f"{args.task} already cancelled")
    return 0
    ctx = Ctx(args)
    task = ctx.ledger.get_task(args.task)
    if task["status"] == "cancelled":
        emit(args, {"ok": True, "task": task}, f"{task['id']} already cancelled")
        return 0
    if args.interrupt:
        atts = ctx.ledger.list_attempts(task_id=task["id"], limit=20)
        for a in atts:
            if a["status"] in ("running", "dispatched", "blocked_permission",
                               "blocked_form", "provider_retry", "stalled"):
                try:
                    ctx.client_or_die().interrupt(a["session_id"])
                except Exception:
                    pass
                ctx.ledger.stop_attempt(a["id"], actor="coordinator")
    task = ctx.ledger.transition_task(
        task["id"], "cancelled", reason=args.reason, actor="coordinator",
        extra={"closed_at": now_ms()},
    )
    emit(args, {"ok": True, "task": task}, f"{task['id']} cancelled")
    return 0


def cmd_dispatch(args) -> int:
    ctx = Ctx(args)
    # 派单 prompt 可能因服务排队较慢；首次请求用更长 HTTP 超时。
    # Client 延迟装配：dry-run/无服务配置也可派单不触网；超时仍按“结果未知”处理。
    task = ctx.ledger.get_task(args.task)
    directory = os.path.abspath(os.path.expanduser(
        args.directory or (task.get("write_scope") or [os.getcwd()])[0]))
    coordinator = _coordinator_from(args)
    user_prompt = None
    if getattr(args, "prompt", None):
        user_prompt = args.prompt
    elif getattr(args, "prompt_file", None):
        user_prompt = _read_text_file(args.prompt_file)
    elif not sys.stdin.isatty() and not args.no_prompt:
        # 允许管道输入；空输入则用默认模板
        data = sys.stdin.read()
        if data.strip():
            user_prompt = data

    collaboration = None
    if getattr(args, "collaboration", None):
        try:
            collaboration = json.loads(args.collaboration)
        except (TypeError, ValueError) as exc:
            raise LedgerError("DELIVERY_COLLABORATION_INVALID", "collaboration必须是JSON对象") from exc
    with ctx.app(client_factory=lambda: Client(timeout=90)) as app:
        out = app.dispatch(
            args.task, directory=directory, coordinator=coordinator,
            session=args.session, model=args.model, agent=args.agent,
            variant=args.variant, scope=list(args.scope or []),
            deliverables=list(args.deliverable or []), checks=list(args.check or []),
            work_mode=args.work_mode,
            prompt=user_prompt, allow_active=args.allow_active,
            ignore_provider_backoff=args.ignore_provider_backoff,
            dry_run=args.dry_run, no_prompt=args.no_prompt,
            deadline=args.deadline, stall_after=args.stall_after,
            takeover_confirmed=args.takeover_confirmed,
            collaboration=collaboration,
        )
    if out.get("dry_run"):
        emit(args, out, f"DRY-RUN task={out['task']} attempt={out['attempt_id']} "
                        f"session={out['session_id']}\n{out['prompt']}")
        return 0
    emit(args, out, f"dispatched task={out['task_id']} attempt={out['attempt_id']} "
                    f"session={out['session_id']}\nmessage={out['message_id']} dir={out['directory']}")
    return 0


def cmd_rework(args) -> int:
    ctx = Ctx(args)
    feedback = args.feedback
    if args.feedback_file:
        feedback = _read_text_file(args.feedback_file)
    coordinator = _coordinator_from(args)
    with ctx.app() as app:
        out = app.rework(args.task, feedback=feedback, session=args.session,
                         coordinator=coordinator, deadline=args.deadline,
                         stall_after=args.stall_after, work_mode=args.work_mode)
    emit(args, out, f"rework task={out['task_id']} attempt={out['attempt_id']} "
                    f"g{out['generation']} session={out['session_id']}")
    return 0


def cmd_attempt_list(args) -> int:
    ctx = Ctx(args)
    atts = ctx.ledger.list_attempts(task_id=args.task)
    emit(args, {"ok": True, "attempts": atts},
         "\n".join(f"{a['id']} {a['task_id']} g{a['generation']} [{a['status']}] "
                   f"session={a.get('session_id')} progress={iso(a.get('last_progress_at'))}"
                   for a in atts) or "(no attempts)")
    return 0


def cmd_attempt_show(args) -> int:
    ctx = Ctx(args)
    att = ctx.ledger.get_attempt(args.attempt)
    out = {"ok": True, "attempt": att, "deliverables": ctx.ledger.deliverables(att["id"]),
           "turns": ctx.ledger.turns(att["id"])}
    emit(args, out, json.dumps(out, ensure_ascii=False, indent=1))
    return 0


def cmd_attempt_stop(args) -> int:
    ctx = Ctx(args)
    att = ctx.ledger.get_attempt(args.attempt)
    if args.confirm or args.external_stopped:
        ctx.ledger.confirm_attempt_stopped(
            att["id"], actor="coordinator", external_stopped=args.external_stopped,
            note=args.note or "manual-confirm",
        )
        emit(args, {"ok": True, "attempt": ctx.ledger.get_attempt(att["id"])},
             f"{att['id']} stop confirmed; site released")
        return 0
    interrupted = None
    if args.interrupt and att.get("session_id"):
        try:
            interrupted = ctx.client_or_die().interrupt(att["session_id"])
        except Exception as e:
            return fail(args, "service", f"interrupt 失败：{e}")
    att = ctx.ledger.stop_attempt(att["id"], actor="coordinator")
    emit(args, {"ok": True, "attempt": att, "interrupt_result": interrupted},
         f"{att['id']} stop requested（受理 ≠ 实际停止；确认后执行 "
         f"attempt release {att['id']} --confirm）")
    return 0


def cmd_attempt_release(args) -> int:
    ctx = Ctx(args)
    att = ctx.ledger.get_attempt(args.attempt)
    if not args.confirm:
        return fail(args, "usage", "必须 --confirm；仅受理停止请求不等于实际停止")
    if not args.external_stopped:
        # 查询失败不能证明外部副作用已停止：保持写权（不调用 release_site），
        # 明确提示人工核实后用 --external-stopped。
        running = []
        try:
            for sh in ctx.client_or_die().shells(att["directory"]):
                if sh.get("status") == "running":
                    sid = (sh.get("metadata") or {}).get("sessionID")
                    if sid == att["session_id"] or sid is None:
                        running.append(sh)
        except OrchError as e:
            return fail(args, "gate",
                        f"无法确认外部副作用已停止（{e}）；人工核实后加 --external-stopped")
        if running:
            return fail(args, "gate",
                        f"仍有 {len(running)} 个运行中的 shell；确认外部副作用已停止后加 "
                        "--external-stopped", {"shells": [s.get("id") for s in running]})
    att = ctx.ledger.release_site(att["id"], confirmed=True,
                                  external_stopped=args.external_stopped,
                                  actor="coordinator", note=args.note or "manual-release")
    emit(args, {"ok": True, "attempt": att}, f"{att['id']} site released")
    return 0


def cmd_collect(args) -> int:
    ctx = Ctx(args)
    carry_manifest = None
    if getattr(args, "carry_manifest", None):
        # CLI 只负责读取 JSON 清单并传 intent；校验在应用用例（D6）
        try:
            with open(os.path.expanduser(args.carry_manifest), "r",
                      encoding="utf-8") as f:
                carry_manifest = json.load(f)
        except (OSError, ValueError) as e:
            emit(args, {"error": f"CARRY_MANIFEST_READ_FAILED: {e}"},
                 f"读取 carry 清单失败：{e}")
            return 2
    with ctx.app() as app:
        out = app.collect(args.attempt, partial=args.partial,
                          carry_manifest=carry_manifest)
    att_id = out["attempt_id"]
    if out.get("already_accepted"):
        emit(args, out, f"{att_id} already accepted; result={out.get('result_path')}")
        return 0
    if out.get("partial"):
        lines = [f"partial diagnostics {att_id} -> {out['status']} "
                 f"({out['text_chars']} chars, msgs={out['assistant_message_count']})",
                 f"diagnostic={out.get('diagnostic_path')}"]
        emit(args, out, "\n".join(lines))
        return 0
    action = "collection pending" if out.get("pending") else "collected"
    lines = [f"{action} {att_id} -> {out['status']} "
             f"({out['text_chars']} chars, msgs={out['assistant_message_count']})",
             f"result={out['result_path']}"]
    if out.get("result_version"):
        lines.append(f"version={out['result_version']} "
                      f"collection_state={out.get('collection_state')}")
    if out.get("classification"):
        lines.append(f"classification={out['classification']}")
    if out.get("ready_for_verification") is not None:
        lines.append(f"ready_for_verification={out['ready_for_verification']}")
    if out.get("notification_ids"):
        lines.append("notifications=" + ", ".join(out["notification_ids"])
                     + "（collect 不隐式 ACK；处理完毕请显式 ack）")
    if out.get("next_collect_at"):
        lines.append(f"pending: next_collect_at={out['next_collect_at']}")
    if out.get("missing"):
        lines.append("MISSING: " + ", ".join(out["missing"]))
    if out.get("stale"):
        lines.append("STALE(未声明沿用): " + ", ".join(out["stale"]))
    if out.get("unverified"):
        lines.append("UNVERIFIED(未执行检查): " + ", ".join(out["unverified"]))
    emit(args, out, "\n".join(lines))
    return 0


def cmd_verify(args) -> int:
    ctx = Ctx(args)
    with ctx.app() as app:
        out = app.verify(
            args.attempt, passed=args.pass_, reason=args.reason or "",
            evidence=list(args.evidence or []), notes=args.notes or "",
            force=bool(args.force), run_checks=bool(args.run_checks),
            check_timeout=args.check_timeout, actor="coordinator",
        )
    task, att = out["task"], out["attempt"]
    verification = out["verification"] or {}
    if out["already"]:
        human = (f"ACCEPTED already {task['id']} attempt={att['id']}" if args.pass_
                 else f"REJECTED already {task['id']} attempt={att['id']}")
        emit(args, out, human)
        return 0
    if args.pass_:
        emit(args, out, f"ACCEPTED {task['id']} attempt={att['id']} "
                        f"evidence={len(verification.get('evidence') or [])}")
    else:
        emit(args, out, f"REJECTED {task['id']} attempt={att['id']}: {args.reason}")
    return 0


def cmd_health(args) -> int:
    """本地只读业务健康；共享投影不以通知ACK/进程心跳替代验收。"""
    from ..composition import make_continuity_status

    conn = init(ensure_home(args.home))
    try:
        ledger = Ledger(conn, args.home)
        out = make_continuity_status(ledger).snapshot(
            project=args.project, session_id=args.session, limit=args.limit,
            after_id=args.after_id, process=supervisor_status(args.home, load_config(args.home)))
    finally:
        conn.close()
    lines = [f"business health: {out['scanned']} tasks; coverage={'complete' if out['coverage']['complete'] else 'partial'}"]
    for item in out["tasks"]:
        lines.append(f"  {item['task_id']} [{item['state']}] owner={item['owner']} next={item['next_action']}")
    emit(args, out, "\n".join(lines))
    return 0


def cmd_incident(args) -> int:
    from ..composition import make_configured_operator_incident

    conn = init(ensure_home(args.home))
    try:
        service = make_configured_operator_incident(
            Ledger(conn, args.home), home=args.home)
        out = service.snapshot(project=args.project, limit=args.limit, after_id=args.after_id)
    finally:
        conn.close()
    emit(args, out, "\n".join(
        f"{item['incident_id']} [{item['state']}] owner={item['owner']} next={item['next_action']} channel={item['channel']}"
        for item in out["incidents"]) or "(no registered incidents; not proof of business completion)")
    return 0


def cmd_email(args) -> int:
    """独立邮件执行器：默认关闭；status/check 零网络，tick 仅显式配置时发送。"""
    from ..composition import (
        inspect_email_config,
        make_email_executor,
        make_operator_email,
    )

    home = ensure_home(args.home)
    info = inspect_email_config(home)
    if not info["ok"]:
        return fail(args, "email-config", info["error"])
    runtime = info["runtime"]
    if args.action == "check":
        out = {"ok": True, "state": runtime.state,
               "source": runtime.source_path,
               "enabled": runtime.enabled,
               "recipient_refs": list(runtime.recipient_refs),
               "projects": sorted(runtime.policy.projects),
               "event_kinds": sorted(runtime.policy.event_kinds)}
        emit(args, out, f"email policy: {runtime.state} "
                        f"recipients={len(runtime.recipient_refs)} "
                        f"projects={len(out['projects'])}")
        return 0
    if args.action == "status":
        conn = init(home)
        try:
            ledger = Ledger(conn, home)
            tick = ledger.get_meta("email-executor:tick")
            status = ledger.get_meta("email-executor:status")
            service = make_operator_email(conn)
            items = service.list_items()
        finally:
            conn.close()
        out = {"ok": True, "tick": tick, "status": status,
               "outbox": items}
        emit(args, out, f"email outbox={len(items)} "
                        f"tick={tick or '-'} status={status or '-'}")
        return 0
    executor, _runtime = make_email_executor(home)
    out = executor.tick()
    emit(args, out, f"email tick ok={out.get('ok')} "
                    f"seq={out.get('seq')} "
                    f"external={out.get('external_calls')}")
    return 0 if out.get("ok") else 1


def cmd_status(args) -> int:
    ctx = Ctx(args)
    sup = supervisor_status(ctx.home, ctx.cfg)
    service = {}
    live = None
    if args.live:
        try:
            client = ctx.client_or_die()
            info = client.info()
            live = {"info": info, "active": client.active_sessions()}
            service = {"ok": True, **client.service.public(), "version": info.get("version")}
        except Exception as e:
            service = {"ok": False, "error": repr(e)}
    else:
        service = {"base": ctx.ledger.get_meta("service_base"),
                   "version": ctx.ledger.get_meta("service_version"),
                   "last_error": ctx.ledger.get_meta("last_service_error")}
    snap = ctx.ledger.status_snapshot(supervisor=sup, service=service)
    if live is not None:
        snap["live"] = live
    if args.json:
        print(json.dumps(snap, ensure_ascii=False, indent=1))
        return 0
    lines = []
    lines.append(f"supervisor: {'healthy' if sup['healthy'] else 'NOT-HEALTHY'} "
                 f"pid={sup.get('pid')} connected={sup.get('connected')} "
                 f"age={sup.get('heartbeat_age_s')}s")
    lines.append(f"service: {service.get('base') or '-'} "
                 f"version={service.get('version') or '-'}")
    running = [t for t in snap["tasks"] if t["status"] in ("running", "dispatched")]
    waiting = [t for t in snap["tasks"] if t["status"] in ("blocked", "stalled")]
    pending = [t for t in snap["tasks"] if t["status"] in ("result_pending", "result_received")]
    other = [t for t in snap["tasks"] if t not in running + waiting + pending]
    if running:
        lines.append("RUNNING:")
        for t in running:
            lines.append(f"  {t['id']} attempt={t['current_attempt']} "
                         f"session={t['session_id']} last={iso(t['last_progress_at'])}")
    if waiting:
        lines.append("WAITING/BLOCKED:")
        for t in waiting:
            lines.append(f"  {t['id']} [{t['status']}:{t.get('blocked_reason')}] "
                         f"attempt={t['current_attempt']} session={t['session_id']} "
                         f"next={iso(t.get('next_check_at'))}")
    if pending:
        lines.append("RESULT-PENDING:")
        for t in pending:
            lines.append(f"  {t['id']} [{t['status']}] attempt={t['current_attempt']} "
                         f"session={t['session_id']}")
    if other:
        lines.append("OTHER:")
        for t in other:
            lines.append(f"  {t['id']} [{t['status']}]")
    if snap["alerts"]:
        lines.append(f"ALERTS ({len(snap['alerts'])} new):")
        for a in snap["alerts"][:10]:
            lines.append(f"  {a['id']} {a['severity']} {a['title']}")
    lines.append(f"notifications: pending={len(snap['notifications'])} "
                 f"sent_unacked={len(snap['sent_unacked'])}")
    if snap.get("provider_backoffs"):
        active = {k: v for k, v in snap["provider_backoffs"].items()
                  if v.get("until", 0) > now_ms()}
        if active:
            lines.append("PROVIDER BACKOFF:")
            for model, st in active.items():
                lines.append(f"  {model} {st.get('category')} until="
                             f"{iso(st.get('until'))}")
    if not snap["tasks"]:
        lines.append("(no tasks)")
    print("\n".join(lines))
    return 0


def cmd_delivery_policy(args) -> int:
    from ..application.delivery_policy_control import DeliveryPolicyControl
    from ..composition import make_delivery_policy

    ctx = Ctx(args)
    try:
        control = DeliveryPolicyControl(make_delivery_policy(ctx.ledger))
        if args.action == "show":
            out = control.show(task_id=args.task, project=args.project)
        else:
            scope = "task" if args.action == "switch-task" else "project"
            target = args.task if scope == "task" else args.project
            body = {"task_id" if scope == "task" else "project": target,
                    "expected_revision": args.expected_revision, "operation_id": args.operation_id,
                    "reason": args.reason, "confirm": args.confirm}
            if getattr(args, "inherit", False):
                if args.policy:
                    raise LedgerError("DELIVERY_POLICY_INVALID", "inherit不能同时携带policy")
                body["inherit"] = True
            else:
                try:
                    body["policy"] = json.loads(args.policy) if args.policy else None
                except (ValueError, TypeError) as exc:
                    raise LedgerError("DELIVERY_POLICY_INVALID", "policy必须是JSON对象") from exc
            out = control.apply(scope, body, actor="operator:cli")
        current = out.get("current", out)
        text = (f"{current['scope']} {current['scope_id']} revision={current['revision']} "
                f"managed={current['managed']} execution_enforced={current['execution_enforced']}\n"
                "仅影响后续安全决策；不抢占在途、不清暂停/冷却；模式不是费用授权。")
        emit(args, out, text)
        return 0
    finally:
        ctx.conn.close()


def cmd_delivery_metrics(args) -> int:
    from ..domain.delivery_metrics import report

    try:
        out = report(json.loads(_read_text_file(args.cohort)), json.loads(_read_text_file(args.observations)), as_of=args.as_of)
    except (TypeError, ValueError) as exc:
        raise LedgerError("DELIVERY_METRICS_INVALID", "cohort/observations须为有效JSON") from exc
    emit(args, out)
    return 0


def cmd_prototype(args) -> int:
    from ..composition import make_prototype

    ctx = Ctx(args)
    try:
        service = make_prototype(ctx.ledger)
        if args.action == "show":
            out = service.show(args.task, args.operation_id)
        else:
            try:
                plan = json.loads(_read_text_file(args.plan_file))
            except (ValueError, TypeError) as exc:
                raise LedgerError("PROTOTYPE_INVALID", "plan-file须为原型计划JSON") from exc
            out = service.apply(args.task, {"plan": plan, "expected_revision": args.expected_revision,
                "operation_id": args.operation_id, "reason": args.reason, "confirm": args.confirm}, actor="operator:cli")
        emit(args, out, f"{out['task_id']} prototype revision={out['revision']} unresolved={len(out['unresolved'])}\n"
             "记录≠修复、原型范围≠正式业务全部验收；不改写原Task承诺。")
        return 0
    finally:
        ctx.conn.close()


def cmd_resource_authorization(args) -> int:
    from ..composition import make_resource_authorization

    ctx = Ctx(args)
    try:
        service = make_resource_authorization(ctx.ledger)
        if args.action == "show":
            out = service.snapshot(args.scope, args.scope_id)
        else:
            if args.confirm != args.scope_id:
                raise LedgerError("RESOURCE_AUTHORIZATION_CONFIRM_REQUIRED", "须逐字回填授权作用域身份")
            try:
                resources = json.loads(args.resources)
            except (TypeError, ValueError) as exc:
                raise LedgerError("RESOURCE_AUTHORIZATION_INVALID", "resources必须为JSON数组") from exc
            out = service.replace(args.scope, args.scope_id, resources=resources,
                expires_at=args.expires_at, expected_revision=args.expected_revision,
                operation_id=args.operation_id, actor="operator:cli", reason=args.reason)
        emit(args, out, json.dumps(out, ensure_ascii=False, indent=1))
        return 0
    finally:
        ctx.conn.close()


def cmd_reconcile(args) -> int:
    if args.dispatch:
        sup = Supervisor(args.home, cfg=load_config(args.home), once=True)
        try:
            sup.run()
        except SupervisorBusy as e:
            return fail(args, "supervisor_busy", str(e))
        emit(args, {"ok": True, "once": True, "dispatch": True},
             "reconcile+dispatch once completed")
        return 0
    sup = Supervisor(args.home, cfg=load_config(args.home), once=True)
    res = sup.cycle(dispatch=False)
    emit(args, {"ok": True, **res}, json.dumps(res, ensure_ascii=False, indent=1))
    return 0


def cmd_inbox(args) -> int:
    ctx = Ctx(args)
    states = ["pending", "inflight", "sent"]
    items = []
    for st in states:
        items.extend(ctx.ledger.list_notifications(target_session=args.session,
                                                    state=st, limit=200))
    items.sort(key=lambda n: (n["priority"], n["created_at"]), reverse=True)
    if args.ack_all:
        ids = [n["id"] for n in items]
        with ctx.app() as app:
            acked = app.ack_notifications(ids)["acked"]
        emit(args, {"ok": True, "acked": acked, "notifications": items},
             f"acked {acked} notification(s) for {args.session}")
        return 0
    if args.json:
        print(json.dumps({"ok": True, "notifications": items}, ensure_ascii=False, indent=1))
        return 0
    if not items:
        print("(no pending notifications)")
        return 0
    for n in items:
        p = n["payload"]
        print(f"{n['id']} [{n['state']}] kind={n['kind']} task={n.get('task_id')} "
              f"attempt={n.get('attempt_id')} attempts={n['attempts']}")
        for key in ("classification", "result_version", "result_path", "collection_state", "summary",
                    "missing_deliverables", "stale_deliverables",
                    "unverified_deliverables", "ready_for_verification",
                    "next_action", "instructions"):
            if p.get(key) is not None:
                print(f"  {key}: {p[key]}")
    print("处理后请显式执行 oc-orch notify ack <ID>（仅确认已读取并处理的通知）")
    return 0


def cmd_notify(args) -> int:
    ctx = Ctx(args)
    if args.action == "list":
        items = ctx.ledger.list_notifications(state=args.state, limit=200)
        emit(args, {"ok": True, "notifications": items},
             "\n".join(f"{n['id']} [{n['state']}] {n['kind']} -> "
                       f"{n['target_session']} task={n.get('task_id')}"
                       for n in items) or "(none)")
        return 0
    if args.action == "ack":
        with ctx.app() as app:
            n = app.ack_notifications(args.ids)["acked"]
        emit(args, {"ok": True, "acked": n}, f"acked {n}")
        return 0
    if args.action == "retry":
        n = ctx.ledger.requeue_notification(args.ids[0], next_attempt_at=now_ms(),
                                           actor="coordinator")
        emit(args, {"ok": True, "notification": n}, f"requeued {n['id']}")
        return 0
    return fail(args, "usage", "notify action 未实现")


def cmd_alerts(args) -> int:
    ctx = Ctx(args)
    if args.action == "list":
        items = ctx.ledger.list_alerts(state=None if args.all else "new", limit=200)
        emit(args, {"ok": True, "alerts": items},
             "\n".join(f"{a['id']} {a['severity']} [{a['state']}] {a['title']} "
                       f"({iso(a['created_at'])})" for a in items) or "(none)")
        return 0
    if args.action == "ack":
        with ctx.app() as app:
            out = app.ack_alerts(args.ids)
        emit(args, out, f"acked {len(out['alerts'])}")
        return 0
    return fail(args, "usage", "alerts action 未实现")


def cmd_coordinator(args) -> int:
    ctx = Ctx(args)
    if args.action == "claim":
        lease = ctx.ledger.coordinator_lease(args.project)
        old_active = False
        if lease and lease["holder_session"] != args.session:
            try:
                old_active = lease["holder_session"] in ctx.client_or_die().active_sessions()
            except Exception:
                old_active = False
        lease = ctx.ledger.claim_coordinator(
            project=args.project, session_id=args.session, actor="coordinator",
            takeover=args.takeover,
            old_confirmed_stopped=args.confirm_old_stopped,
            old_session_active=old_active, ttl=args.ttl,
        )
        retargeted = ctx.ledger.retarget_tasks(args.project, args.session,
                                               actor="coordinator")
        emit(args, {"ok": True, "lease": lease, "retargeted_tasks": retargeted},
             f"coordinator project={args.project} holder={lease['holder_session']} "
             f"epoch={lease['epoch']} retargeted={retargeted}")
        return 0
    if args.action == "release":
        ctx.ledger.release_coordinator(args.project, actor="coordinator")
        emit(args, {"ok": True}, f"released {args.project}")
        return 0
    if args.action == "status":
        rows = ctx.ledger.list_coordinator_leases()
        emit(args, {"ok": True, "leases": rows},
             "\n".join(f"{r['project']} holder={r['holder_session']} epoch={r['epoch']} "
                       f"failures={r['failure_count']} blocked={r['recovery_blocked']}"
                       for r in rows) or "(no leases)")
        return 0
    if args.action == "recover":
        lease = ctx.ledger.coordinator_lease(args.project)
        if not lease:
            return fail(args, "usage", f"项目 {args.project} 没有 coordinator 租约")
        target = args.session or lease["holder_session"]
        n = ctx.ledger.enqueue_notification(
            kind="coordinator_manual_recovery", target_session=target,
            dedupe_key=f"coord-manual-recovery:{args.project}:{now_ms()}",
            payload={
                "project": args.project, "session_id": target,
                "classification": "COORDINATOR_MANUAL_RECOVERY",
                "summary": "人工请求从台账检查点恢复主协调",
                "instructions": args.message or (
                    f"运行 oc-orch status --json 与 oc-orch inbox "
                    f"--session {target} --ack-all；只续接未完成责任"
                ),
            },
            project=args.project, priority=10, max_attempts=1,
        )
        emit(args, {"ok": True, "notification": n}, f"queued recovery {n['id']}")
        return 0
    if args.action == "unlock":
        gate = make_target_gate(ctx.conn, SystemClock())
        try:
            out = unlock_manual_gate(
                ctx.ledger, gate, project=args.project, session=args.session,
                reason=args.reason or "manual-recovery", actor="operator",
                now=now_ms(),
                allow_provider=bool(getattr(args, "allow_provider", False)))
        except TargetGateStateError as e:
            return fail(args, "gate", f"闸门状态损坏，拒绝解锁：{e}")
        if out.get("cleared"):
            via = out.get("cleared_via", "manual")
            emit(args, out,
                 (f"restored scheduling for {out['session']}"
                  if via == "provider" else
                  f"unlocked manual gate for {out['session']}")
                 + f"; resurfaced={out.get('resurfaced', 0)} "
                   f"acked={len(out.get('acked_alerts') or [])}")
        else:
            emit(args, out, f"no active gate for {out['session']}")
        return 0
    if args.action == "block":
        lease = ctx.ledger.coordinator_lease(args.project)
        if not lease:
            return fail(args, "usage", f"项目 {args.project} 没有 coordinator 租约")
        # 显式项目暂停必须使用独立的持久 reason，不能被“用户下一次直接输入”
        # 自动解除；回合中断由 supervisor 写 coordinator-interrupted。
        gate = make_target_gate(ctx.conn, SystemClock())
        try:
            gate.block_manual(
                lease["holder_session"], reason="coordinator-paused")
        except TargetGateStateError as e:
            return fail(args, "gate", f"闸门状态损坏，拒绝暂停：{e}")
        ctx.ledger.audit(
            "operator", "target-gate.block-paused", "session",
            lease["holder_session"],
            {"project": args.project, "reason": args.reason or "manual-block"})
        ctx.ledger.block_coordinator_recovery(
            args.project, reason=args.reason or "manual-block"
        )
        count = ctx.ledger.defer_notifications_for_session(
            lease["holder_session"],
            next_attempt_at=now_ms() + 3600 * 1000,
            reason=f"coordinator-blocked:{args.reason or 'manual'}",
        )
        emit(args, {"ok": True, "lease": ctx.ledger.coordinator_lease(args.project),
                    "deferred_notifications": count,
                    "gate": gate.get(lease["holder_session"])},
             f"blocked coordinator {args.project}; deferred={count}")
        return 0
    if args.action in _CONTROL_METHOD:
        # 显式本地 operator 控制入口：经共享用例转 CoordinatorControlService。
        # 只接受显式 project/session、operation-id、reason 与 CAS；缺任一拒绝，
        # 不清 provider 门、不伪造 ACK/验收；service 拒绝时如实上报未执行。
        method = _CONTROL_METHOD[args.action]
        try:
            kw = _control_kwargs(args)
            with ctx.coordinator_control() as cc:
                from ..application.conversation_control import ConversationControl
                out = getattr(ConversationControl(cc), method)(**kw)
        except ActionError as e:
            return fail(args, e.code, e.message, e.detail)
        emit(args, out, _control_human(args.action, out, args))
        return 0
    if args.action == "control":
        # 只读状态：项目意图/会话 hold/回合/自动资格；不触发任何写入。
        if not (args.project or args.session):
            return fail(args, "usage",
                        "control 至少需要 --project 或 --session 之一",
                        {"project": args.project, "session": args.session})
        with ctx.coordinator_control() as cc:
            from ..application.conversation_control import ConversationControl
            out = ConversationControl(cc).status(
                project=args.project, session_id=args.session)
        emit(args, out, _control_status_human(out))
        return 0
    return fail(args, "usage", "coordinator action 未实现")


def cmd_conversation(args) -> int:
    from ..composition import make_operator_turn

    ctx = Ctx(args)
    try:
        service = make_operator_turn(ctx.ledger, client=ctx.client_or_die)
        body = {"project": args.project, "session_id": args.session, "operation_id": args.operation_id}
        if args.action == "send":
            body.update(text=args.text, reason=args.reason, confirm=args.confirm,
                        expected_revision=args.expected_revision, expected_epoch=args.expected_epoch)
            result = service.send(body)
        else:
            result = getattr(service, args.action)(**body)
        emit(args, result)
        return 0
    finally:
        ctx.conn.close()


def cmd_send(args) -> int:
    """系统 send 默认登记托管输入与后续责任；纯通信须显式 --untracked。"""
    ctx = Ctx(args)
    text = args.text or (_read_text_file(args.file) if args.file else None)
    if not text:
        return fail(args, "usage", "send 需要 --text 或 --file")
    materials = None
    if getattr(args, "material", None):
        materials = list(args.material)
    kw = dict(
        text=text,
        message_id=args.message_id,
        owner=args.owner,
        task_id=args.task,
        attempt_id=args.attempt,
        target_session=args.continuation_target,
        deadline=args.deadline,
        materials=materials,
        work_kind=args.work_kind,
        delivery=args.delivery,
        untracked=bool(args.untracked),
    )
    if args.same_candidate:
        kw["same_candidate"] = True
    with ctx.managed() as mi:
        out = mi.send_tracked(args.session, **kw)
    inp = out.get("input") or {}
    receipt = out.get("receipt") or inp
    msg_id = _view_field(receipt, "message_id", args.message_id or "-")
    tracking = out.get("tracking") or receipt.get("tracking") or "untracked"
    if tracking == "tracked" or receipt.get("responsibility_id"):
        human = (f"accepted {_view_field(receipt, 'input_id')} session={args.session} "
                 f"message={msg_id} tracking={tracking} "
                 f"responsibility={receipt.get('responsibility_id') or '-'} "
                 f"target={receipt.get('target_session') or '-'}\n"
                 "受理≠投递≠执行≠业务ACK；执行结束后形成接续待办（awaiting_review），"
                 "交付验收仍须 collect/verify")
    else:
        human = (f"sent {_view_field(receipt, 'input_id')} session={args.session} "
                 f"message={msg_id} tracking=untracked\n"
                 f"未托管边界：{receipt.get('boundary') or out.get('boundary') or '-'}"
                 "\n纯通信不授予任务写权，也不保证执行后接续；"
                 "需要托管请提供可证明的 task/owner/target")
    payload = {"ok": True, "message": inp, "message_id": msg_id,
               "tracking": tracking, "input": inp, "receipt": receipt,
               "responsibility": out.get("responsibility")}
    if out.get("boundary") or receipt.get("boundary"):
        payload["boundary"] = out.get("boundary") or receipt.get("boundary")
    emit(args, payload, human)
    return 0


# ---- 输入命令：受管输入（S4）-----------------------------------------
def _view_field(v, key, default="-"):
    """从共享用例视图 dict 读展示字段；只做字典读取，不做语义判定。"""
    if not isinstance(v, dict) or key not in v:
        return default
    val = v[key]
    if val is None:
        return default
    return val


def _view_session(v):
    return _view_field(v, "session_id", _view_field(v, "session"))


def _fmt_pending_age(ms) -> str:
    if ms is None:
        return "-"
    try:
        ms = int(ms)
    except (TypeError, ValueError):
        return str(ms)
    if ms < 0:
        return "-"
    sec = ms // 1000
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m{sec % 60}s"
    if sec < 86400:
        return f"{sec // 3600}h{(sec % 3600) // 60}m"
    return f"{sec // 86400}d{(sec % 86400) // 3600}h"


def _redact_list(obj):
    """列表展示不泄露原正文：剔除 text/body/body_hash 等正文携带字段。"""
    if not isinstance(obj, dict):
        return obj
    out = dict(obj)
    for key in ("inputs", "watches", "observed", "observations", "results",
                "responsibilities"):
        items = out.get(key)
        if not isinstance(items, list):
            continue
        fresh = []
        for item in items:
            if isinstance(item, dict):
                fresh.append({k: v for k, v in item.items()
                              if k not in ("text", "body", "body_hash")})
            else:
                fresh.append(item)
        out[key] = fresh
    return out


def _observed_row_line(obs) -> str:
    target = _view_field(obs, "session_id", _view_field(obs, "session"))
    trusted = obs.get("trusted")
    trust = ("trusted" if trusted is True
             else "untrusted" if trusted is False else "-")
    pending = obs.get("pending")
    blocking = obs.get("blocking_reason")
    parts = [f"observe {target}",
             f"owner={_view_field(obs, 'owner')}",
             f"trusted={trust}",
             f"pending={'-' if pending is None else pending}",
             f"oldest_age={_fmt_pending_age(obs.get('oldest_age_ms'))}",
             f"aged_out={'yes' if obs.get('aged_out') else 'no'}"]
    if blocking is not None:
        parts.append(f"blocking={blocking}")
    if obs.get("error") is not None:
        parts.append(f"error={obs.get('error')}")
    parts.append(f"next={_view_field(obs, 'next_action')}")
    return " ".join(parts)


def _input_row_line(inp) -> str:
    return (f"{_view_field(inp, 'input_id')} "
            f"admission={_view_field(inp, 'admission')} "
            f"delivery={_view_field(inp, 'delivery')} "
            f"execution={_view_field(inp, 'execution')} "
            f"business_ack={_view_field(inp, 'business_ack')} "
            f"intent={_view_field(inp, 'intent')} session={_view_session(inp)} "
            f"owner={_view_field(inp, 'owner')} "
            f"age={_fmt_pending_age(inp.get('pending_age_ms'))} "
            f"next={_view_field(inp, 'next_action')}")


def _watch_row_line(w) -> str:
    enabled = w.get("enabled")
    state = ("enabled" if enabled is True
             else "disabled" if enabled is False else "-")
    parts = [f"watch {_view_field(w, 'session_id', _view_field(w, 'session'))}",
             f"state={state}",
             f"owner={_view_field(w, 'owner')}",
             f"project={_view_field(w, 'project')}",
             f"max_pending_age_ms={_view_field(w, 'max_pending_age_ms')}",
             f"revision={_view_field(w, 'revision')}"]
    if w.get("pending_count") is not None:
        parts.append(f"pending={w.get('pending_count')}")
    if w.get("next_action") is not None:
        parts.append(f"next={w.get('next_action')}")
    obs = w.get("last_observation")
    if isinstance(obs, dict) and obs:
        trust = ("trusted" if obs.get("trusted") is True
                 else "untrusted" if obs.get("trusted") is False else "unknown")
        pending = obs.get("pending")
        parts.append(f"obs.pending={'-' if pending is None else pending}")
        parts.append(f"obs.oldest_age="
                     f"{_fmt_pending_age(obs.get('oldest_age_ms'))}")
        parts.append(f"obs.trusted={trust}")
        if obs.get("aged_out") is not None:
            parts.append(f"obs.aged_out={'yes' if obs.get('aged_out') else 'no'}")
        if obs.get("stale") is not None:
            parts.append(f"obs.stale={'yes' if obs.get('stale') else 'no'}")
        if obs.get("blocking_reason") is not None:
            parts.append(f"obs.blocking={obs.get('blocking_reason')}")
        if obs.get("next_action") is not None:
            parts.append(f"obs.next={obs.get('next_action')}")
    else:
        parts.append("observation=unknown(未观察)")
    last_at = w.get("last_observed_at")
    if last_at is not None:
        parts.append(f"last_observed_at={last_at} ({iso(last_at) or '-'})")
    return " ".join(parts)


def _gate_summary(gate) -> str:
    """渲染 target gate 快照的关键剩余限制；不承诺已执行或可自动发送。"""
    if not isinstance(gate, dict):
        return "gate=-"
    state = gate.get("state") or "-"
    source = gate.get("source") or "-"
    exhausted = "yes" if gate.get("exhausted") else "no"
    fc = gate.get("failure_count")
    pc = gate.get("probe_count")
    cap = gate.get("max_half_open_probes")
    probe = (f"{pc}/{cap}" if pc is not None and cap is not None
             else str(pc if pc is not None else "-"))
    until = gate.get("blocked_until")
    until_txt = "-" if until is None else f"{until} ({iso(until) or '-'})"
    return (f"gate.state={state} source={source} blocked_until={until_txt} "
            f"exhausted={exhausted} failure_count={fc if fc is not None else '-'} "
            f"probe_budget={probe}")


def cmd_input_send(args) -> int:
    ctx = Ctx(args)
    text = args.text or (_read_text_file(args.file) if args.file else None)
    if not text:
        return fail(args, "usage", "input send 需要 --text 或 --file")
    kw = dict(intent=args.intent, text=text, owner=args.owner,
              request_id=args.request_id, project=args.project or "")
    # 与顶层 send 对齐：已托管 task 分支由共享用例委托 send_tracked，
    # 入口只透传显式参数，绝不在这里复制跟踪/写权判定。
    if args.task:
        kw["task_id"] = args.task
    if args.attempt:
        kw["attempt_id"] = args.attempt
    if args.continuation_target:
        kw["target_session"] = args.continuation_target
    if args.deadline is not None:
        kw["deadline"] = args.deadline
    if getattr(args, "material", None):
        kw["materials"] = list(args.material)
    if args.work_kind is not None:
        kw["work_kind"] = args.work_kind
    if args.untracked:
        kw["untracked"] = True
    if args.same_candidate:
        kw["same_candidate"] = True
    if args.intent == "park":
        if args.expires_at is None or not (args.flush_condition or "").strip():
            return fail(args, "usage",
                        "park 必须同时提供 --expires-at（毫秒整数）与 --flush-condition")
        kw["expires_at"] = args.expires_at
        kw["flush_condition"] = args.flush_condition
    with ctx.managed() as mi:
        out = mi.send(args.session, **kw)
    inp = out.get("input") or {}
    inp_id = _view_field(inp, "input_id")
    if args.intent == "record":
        human = (f"recorded {inp_id} session={_view_session(inp)} intent=record"
                 "；已留档，无发送阶段")
    else:
        tracking = out.get("tracking") or inp.get("tracking")
        extra = ""
        if tracking == "tracked" or inp.get("responsibility_id"):
            extra = (" tracking=tracked "
                     f"responsibility={inp.get('responsibility_id') or '-'}")
        elif tracking == "untracked":
            extra = (" tracking=untracked（未托管边界：" +
                     str(out.get("boundary") or inp.get("boundary")
                         or "不授予任务写权/无执行后接续") + "）")
        human = (f"accepted {inp_id} session={_view_session(inp)} "
                 f"intent={args.intent} message={_view_field(inp, 'message_id')}"
                 f"{extra}\n"
                 f"受理≠投递≠执行≠业务ACK；用 `oc-orch input show {inp_id}` 核对回执")
    emit(args, out, human)
    return 0


def cmd_input_list(args) -> int:
    ctx = Ctx(args)
    with ctx.managed() as mi:
        out = mi.list_inputs(session_id=args.session or None, limit=args.limit)
    safe = _redact_list(out)
    rows = ("\n".join(_input_row_line(i) for i in safe.get("inputs") or [])
            or "(no inputs)")
    emit(args, safe, rows + "\n列表不含正文；受理≠投递≠执行≠业务ACK")
    return 0


def cmd_input_show(args) -> int:
    ctx = Ctx(args)
    with ctx.managed() as mi:
        out = mi.get(args.input)
    inp = out.get("input") or {}
    lines = [
        f"input {_view_field(inp, 'input_id', args.input)} "
        f"session={_view_session(inp)} intent={_view_field(inp, 'intent')} "
        f"owner={_view_field(inp, 'owner')}",
        (f"  admission={_view_field(inp, 'admission')} "
         f"delivery={_view_field(inp, 'delivery')} "
         f"execution={_view_field(inp, 'execution')} "
         f"business_ack={_view_field(inp, 'business_ack')}"),
        (f"  message={_view_field(inp, 'message_id')} "
         f"resume={_view_field(inp, 'resume')} revision={_view_field(inp, 'revision')}"),
    ]
    tracking = inp.get("tracking")
    if tracking:
        lines.append(
            f"  tracking={tracking} task={inp.get('task_id') or '-'} "
            f"attempt={inp.get('attempt_id') or '-'} "
            f"generation={inp.get('generation') if inp.get('generation') is not None else '-'} "
            f"result_version={inp.get('result_version') or '-'} "
            f"target={inp.get('target_session') or '-'} "
            f"responsibility={inp.get('responsibility_id') or '-'}")
        if tracking == "untracked":
            lines.append(
                "  未托管边界：纯通信/只读观察，不授予写权，无执行后接续保证；"
                "外部 watch 不创建 Task/Attempt")
    if inp.get("expires_at") is not None:
        at = iso(inp.get("expires_at"))
        lines.append("  expires_at=%s%s" % (inp["expires_at"], f" ({at})" if at else ""))
    if inp.get("pending_age_ms") is not None:
        lines.append(f"  pending_age={_fmt_pending_age(inp.get('pending_age_ms'))}")
    if inp.get("next_action") is not None:
        lines.append(f"  next_action={inp.get('next_action')}")
    text = inp.get("text")
    if text is None:
        text = inp.get("body")
    if text is not None:
        shown = str(text)
        lines.append(f"  text={shown[:200]}{'…<truncated>' if len(shown) > 200 else ''}")
    lines.append("  本回执不证明已投递/执行；业务完成需显式 ACK/验收")
    emit(args, out, "\n".join(lines))
    return 0


def cmd_input_reconcile(args) -> int:
    ctx = Ctx(args)
    with ctx.managed() as mi:
        out = mi.reconcile(args.input)
    inp = out.get("input") or {}
    emit(args, out,
         f"reconciled {args.input} admission={_view_field(inp, 'admission')} "
         f"delivery={_view_field(inp, 'delivery')} execution={_view_field(inp, 'execution')} "
         f"business_ack={_view_field(inp, 'business_ack')}\n"
         "受理≠投递≠执行≠业务ACK；具体证据以回执展示为准")
    return 0


def cmd_input_ack(args) -> int:
    """显式 bool 业务 ACK：false 不是 ACK；通知 ACK 不自动业务 ACK。"""
    value = args.value == "true"
    if not args.confirm or args.confirm != args.input:
        return fail(args, "usage",
                    "ack 必须 --confirm <输入ID> 精确确认对象",
                    {"expected": args.input, "received": args.confirm})
    if not (args.owner or "").strip() or not (args.reason or "").strip():
        return fail(args, "usage", "ack 必须提供非空 --owner 与 --reason")
    if args.expected_revision is None:
        return fail(args, "usage", "ack 必须提供 --expected-revision")
    ctx = Ctx(args)
    with ctx.managed() as mi:
        ack = getattr(mi, "acknowledge", None)
        if not callable(ack):
            return fail(args, "usage",
                        "共享用例尚未提供 acknowledge；不得以通知 ACK 冒充业务 ACK")
        out = ack(args.input, value=value, owner=args.owner,
                  confirm=args.confirm,
                  expected_revision=args.expected_revision,
                  reason=args.reason)
    inp = out.get("input") or {}
    emit(args, out,
         f"business-ack {args.input} value={'true' if value else 'false'} "
         f"state={_view_field(inp, 'business_ack')}\n"
         "value=false 不构成 ACK；本命令不改变通知收件状态，"
         "通知 ACK 也不自动业务 ACK/验收")
    return 0


def _input_mutation(args, action: str) -> int:
    target = args.inputid
    if not args.confirm or args.confirm != target:
        return fail(args, "usage",
                    f"{action} 必须 --confirm <对象ID> 明确确认（收到 {args.confirm!r}）",
                    {"expected": target, "received": args.confirm})
    if not (args.owner or "").strip() or not (args.reason or "").strip():
        return fail(args, "usage", f"{action} 必须提供非空 --owner 与 --reason")
    if args.expected_revision is None:
        return fail(args, "usage", f"{action} 必须提供 --expected-revision")
    ctx = Ctx(args)
    with ctx.managed() as mi:
        out = getattr(mi, action)(target, owner=args.owner, confirm=args.confirm,
                                  expected_revision=args.expected_revision,
                                  reason=args.reason)
    inp = out.get("input") or {}
    human = (f"{action} requested {target} owner={args.owner} "
             f"admission={_view_field(inp, 'admission')}\n"
             "受理≠投递≠执行≠业务ACK；远端无响应/404 不等于成功，"
             f"用 `oc-orch input show {target}` 核对")
    emit(args, out, human)
    return 0


def cmd_input_flush(args) -> int:
    return _input_mutation(args, "flush")


def cmd_input_cancel(args) -> int:
    return _input_mutation(args, "cancel")


def cmd_input_resume_target(args) -> int:
    if not args.confirm or args.confirm != args.session:
        return fail(args, "usage",
                    "resume-target 必须 --confirm <目标session> 人工确认恢复对象",
                    {"expected": args.session, "received": args.confirm})
    if not (args.owner or "").strip() or not (args.reason or "").strip():
        return fail(args, "usage", "resume-target 必须提供非空 --owner 与 --reason")
    if args.expected_revision is None:
        return fail(args, "usage", "resume-target 必须提供 --expected-revision")
    ctx = Ctx(args)
    with ctx.managed() as mi:
        out = mi.resume_target(args.session, owner=args.owner, confirm=args.confirm,
                               expected_revision=args.expected_revision,
                               reason=args.reason)
    emit(args, out,
         f"人工控制恢复请求已处理 session={args.session}\n"
         f"  {_gate_summary(out.get('gate'))}\n"
         "仅解除明确人工控制：不清额度/probe 预算、不立即触发 prompt；"
         "后续自动发送仍受 gate/额度/队列限制，未验证执行")
    return 0


def cmd_input_watch(args) -> int:
    ctx = Ctx(args)
    if args.action == "register":
        if args.max_pending_age is None or args.max_pending_age < 0:
            return fail(args, "usage",
                        "watch register 的 --max-pending-age（秒）必须为非负整数")
        with ctx.managed() as mi:
            out = mi.register_watch(
                args.session, owner=args.owner, project=args.project,
                max_pending_age_ms=args.max_pending_age * 1000)
        emit(args, out,
             f"watch registered {args.session} owner={args.owner} "
             f"project={args.project} max_pending_age={args.max_pending_age}s\n"
             "仅登记观察目标；不自动接管，也不触发远端执行")
        return 0
    if args.action == "unregister":
        if not args.confirm or args.confirm != args.session:
            return fail(args, "usage",
                        "watch unregister 必须 --confirm <session> 确认撤销对象",
                        {"expected": args.session, "received": args.confirm})
        with ctx.managed() as mi:
            out = mi.unregister_watch(args.session, owner=args.owner,
                                      expected_revision=args.expected_revision,
                                      confirm=args.confirm)
        emit(args, out, f"watch unregistered {args.session}")
        return 0
    if args.action == "list":
        with ctx.managed() as mi:
            out = mi.list_watches(limit=args.limit)
        safe = _redact_list(out)
        rows = ("\n".join(_watch_row_line(w) for w in safe.get("watches") or [])
                or "(no watches)")
        emit(args, safe, rows)
        return 0
    if args.action == "observe":
        with ctx.managed() as mi:
            out = mi.observe_registered(limit=args.limit)
        safe = _redact_list(out)
        items = (safe.get("observed") or safe.get("watches")
                 or safe.get("observations") or [])
        rows = ("\n".join(_observed_row_line(o) for o in items)
                or "(no observations)")
        emit(args, safe, rows + "\n仅白名单定点观察；不扫描全局 session/消息")
        return 0
    return fail(args, "usage", "input watch action 未实现")


# ---- 后续责任命令（S4 连续性）-----------------------------------------
def _resp_row_line(r) -> str:
    return (f"{_view_field(r, 'id')} state={_view_field(r, 'state')} "
            f"task={_view_field(r, 'task_id')} owner={_view_field(r, 'owner')} "
            f"target={_view_field(r, 'target_session')} "
            f"wait={_view_field(r, 'wait_kind')}/{_view_field(r, 'wait_condition')} "
            f"next_check={r.get('next_check_at')} "
            f"deadline={r.get('deadline')} budget={r.get('budget_used')}/"
            f"{r.get('max_budget')} rev={_view_field(r, 'revision')}"
            f"\n  next={_view_field(r, 'next_action')}")


def cmd_responsibility(args) -> int:
    ctx = Ctx(args)
    if args.action == "list":
        with ctx.responsibilities() as store:
            rows = store.list_open_responsibilities(
                task_id=args.task or None, owner=args.owner or None,
                limit=args.limit)
        payload = {"ok": True, "responsibilities": rows}
        emit(args, payload,
             ("\n".join(_resp_row_line(r) for r in rows)
              or "(no open responsibilities)")
             + "\n职责只表达谁在等待什么；ACK 不等于责任完成")
        return 0
    if args.action == "show":
        with ctx.responsibilities() as store:
            row = store.get_responsibility(args.responsibility)
        if row is None:
            return fail(args, "usage", f"责任不存在：{args.responsibility}")
        emit(args, {"ok": True, "responsibility": row},
             _resp_row_line(row) + f"\n  evidence={json.dumps(row.get('evidence') or {}, ensure_ascii=False)}")
        return 0
    if args.action == "transfer":
        if args.expected_revision is None or not (args.owner or "").strip():
            return fail(args, "usage",
                        "transfer 必须提供 --owner（新责任人）与 --expected-revision")
        successor = {
            "owner": args.owner,
            "state": args.state,
            "wait_kind": args.wait_kind,
            "wait_condition": args.wait_condition,
            "next_action": args.next_action,
            "next_check_at": args.next_check_at or now_ms(),
            "condition_key": args.condition_key or None,
            "target_session": args.target,
        }
        successor = {k: v for k, v in successor.items() if v is not None}
        with ctx.responsibilities() as store:
            out = store.transfer_responsibility(
                args.responsibility, expected_revision=args.expected_revision,
                successor=successor, reason=args.reason or "manual-transfer",
                release_manual_confirmation=args.release_manual_confirm)
        emit(args, {"ok": True, **out},
             f"transferred {args.responsibility} -> "
             f"{_view_field(out.get('successor') or {}, 'id')} owner={args.owner}")
        return 0
    if args.action == "resume":
        if not args.confirm or args.confirm != args.responsibility:
            return fail(args, "usage",
                        "resume 必须 --confirm <责任ID> 显式确认人工恢复",
                        {"expected": args.responsibility,
                         "received": args.confirm})
        if args.expected_revision is None or not (args.reason or "").strip():
            return fail(args, "usage",
                        "resume 必须提供 --expected-revision 与非空 --reason")
        with ctx.responsibilities() as store:
            row = store.resume_responsibility(
                args.responsibility, expected_revision=args.expected_revision,
                confirm=args.confirm, reason=args.reason, actor="human")
        emit(args, {"ok": True, "responsibility": row},
             f"resumed {args.responsibility} state={row.get('state')} "
             f"revision={row.get('revision')}\n"
             "人工确认只离开 waiting_human 回到 handling；"
             "不授予写权、不解除 task 暂停/取消或目标闸门，也不自动发送")
        return 0
    if args.action == "wait":
        if args.expected_revision is None:
            return fail(args, "usage", "wait 必须提供 --expected-revision")
        if not (args.wait_condition or "").strip():
            return fail(args, "usage", "wait 必须提供 --wait-condition 说明等待对象")
        fields = {
            "state": args.state,
            "wait_kind": args.wait_kind,
            "wait_condition": args.wait_condition,
            "next_action": args.next_action or "等待条件满足后复核 current/人工控制",
            "next_check_at": args.next_check_at or now_ms() + 300 * 1000,
        }
        if args.deadline is not None:
            fields["deadline"] = args.deadline
        with ctx.responsibilities() as store:
            row = store.update_responsibility(
                args.responsibility, expected_revision=args.expected_revision,
                fields=fields, actor="operator")
        emit(args, {"ok": True, "responsibility": row},
             _resp_row_line(row))
        return 0
    if args.action == "close":
        if args.expected_revision is None or not (args.reason or "").strip():
            return fail(args, "usage",
                        "close 必须提供 --expected-revision 与 --reason")
        with ctx.responsibilities() as store:
            row = store.get_responsibility(args.responsibility)
            if row is None:
                return fail(args, "usage",
                            f"责任不存在：{args.responsibility}")
            base = getattr(store, "base", None)
            task = None
            if base is not None:
                try:
                    task = base.get_task(row.get("task_id"))
                except Exception:
                    task = None
            status = (task or {}).get("status")
            terminal = (status in ("accepted", "cancelled")
                        or (task or {}).get("closed_at") is not None)
            if not terminal and not args.superseded_by:
                # 不准以 manual-close/superseded 等文本原因关掉未完成业务推进。
                return fail(
                    args, "usage",
                    "非终态任务不能以文本原因关闭推进责任：请先 "
                    "`oc-orch task cancel`，或用 `responsibility wait` 登记"
                    "人工/依赖等待、`responsibility transfer` 转交后继责任；"
                    "已有持久后继时可 --superseded-by <责任ID> 交由 store 事实守卫",
                    {"task_id": row.get("task_id"), "task_status": status,
                     "options": ["task cancel", "responsibility wait",
                                 "responsibility transfer",
                                 "close --superseded-by <id>"]})
            row = store.close_responsibility(
                args.responsibility, expected_revision=args.expected_revision,
                reason=args.reason, actor="operator",
                superseded_by=args.superseded_by)
        emit(args, {"ok": True, "responsibility": row},
             f"closed {args.responsibility} state={row.get('state')} "
             f"reason={row.get('close_reason')}\n"
             "关闭推进责任需业务 accepted/cancelled 或显式人工/转交原因；"
             "ACK、succeeded、空 inbox 不构成依据")
        return 0
    return fail(args, "usage", "responsibility action 未实现")


def cmd_permission(args) -> int:
    ctx = Ctx(args)
    if args.action == "list":
        pending = ctx.client_or_die().permissions(args.directory)
        if args.session:
            pending = [p for p in pending if p.get("sessionID") == args.session]
        emit(args, {"ok": True, "permissions": pending},
             "\n".join(f"{p['id']} session={p['sessionID']} {p.get('action')} "
                       f"{p.get('resources')}" for p in pending) or "(none)")
        return 0
    if args.action == "reply":
        ctx.client_or_die().reply_permission(
            args.session, args.request, args.decision, args.message
        )
        ctx.ledger.audit("coordinator", "permission.reply", "permission", args.request,
                         {"decision": args.decision, "session": args.session})
        emit(args, {"ok": True, "request": args.request, "decision": args.decision},
             f"replied {args.request} -> {args.decision}")
        return 0
    return fail(args, "usage", "permission action 未实现")


def cmd_export(args) -> int:
    ctx = Ctx(args)
    data = ctx.ledger.export()
    text = json.dumps(data, ensure_ascii=False, indent=1)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(args.out, 0o600)
        emit(args, {"ok": True, "out": args.out, "bytes": len(text)},
             f"exported {args.out} ({len(text)} bytes)")
    else:
        print(text)
    return 0


def cmd_web(args) -> int:
    from .web import PID_NAME, WebServer

    home = ensure_home(args.home)
    pid_path = os.path.join(home, PID_NAME)
    if args.stop:
        try:
            with open(pid_path, "r", encoding="utf-8") as f:
                info = json.load(f)
            pid = int(info.get("pid"))
        except (OSError, ValueError, TypeError):
            emit(args, {"ok": True, "stopped": False, "reason": "not-running"},
                 "web not running")
            return 0
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                os.kill(pid, 0)
            except OSError:
                break
            time.sleep(0.2)
        try:
            os.unlink(pid_path)
        except OSError:
            pass
        emit(args, {"ok": True, "stopped": True, "pid": pid},
             f"web stopped pid={pid}")
        return 0
    if args.host not in ("127.0.0.1", "localhost", "::1"):
        return fail(args, "usage",
                    "Web 只允许 loopback（127.0.0.1/localhost/::1）；"
                    "不要绑定到外部地址")
    if args.daemon:
        try:
            with open(pid_path, "r", encoding="utf-8") as f:
                old = json.load(f)
            pid = int(old.get("pid"))
            os.kill(pid, 0)
            return fail(args, "usage",
                        f"web 已在运行 pid={pid} port={old.get('port')}")
        except (OSError, ValueError, TypeError):
            pass
        log_path = os.path.join(home, "web.log")
        env = os.environ.copy()
        env["OC_ORCH_HOME"] = home
        root = _module_root()
        if root:
            env["PYTHONPATH"] = root + (os.pathsep + env["PYTHONPATH"]
                                        if env.get("PYTHONPATH") else "")
        log = open(log_path, "a", encoding="utf-8")
        cmd = [sys.executable, "-m", "oc_orch.cli", "--home", home,
               "web", "--host", args.host, "--port", str(args.port)]
        if args.read_only:
            cmd.append("--read-only")
        p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                             start_new_session=True, env=env)
        deadline = time.time() + 6
        info = None
        while time.time() < deadline:
            try:
                with open(pid_path, "r", encoding="utf-8") as f:
                    info = json.load(f)
                break
            except (OSError, ValueError):
                time.sleep(0.2)
        if not info:
            return fail(args, "service", f"web 启动失败，见 {log_path}")
        port = info.get("port")
        emit(args, {"ok": True, "pid": p.pid, "port": port,
                    "url": f"http://127.0.0.1:{port}/",
                    "log": log_path, "read_only": bool(info.get("read_only"))},
             f"web listening http://127.0.0.1:{port}/  pid={p.pid} log={log_path}")
        return 0
    server = WebServer(home, host=args.host, port=args.port,
                       read_only=args.read_only, verbose=args.verbose)
    try:
        port = server.start()
        print(f"oc-orch web listening http://127.0.0.1:{port}/", flush=True)
        if args.read_only:
            print("mode: read-only", flush=True)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
    return 0


def cmd_supervisor(args) -> int:
    if args.action == "status":
        st = supervisor_status(args.home, load_config(args.home))
        emit(args, {"ok": True, "supervisor": st},
             json.dumps(st, ensure_ascii=False, indent=1))
        return 0
    if args.action == "start":
        st = supervisor_status(args.home, load_config(args.home))
        if st["healthy"]:
            emit(args, {"ok": True, "already_running": True, "supervisor": st},
                 f"supervisor already running pid={st['pid']}")
            return 0
        home = ensure_home(args.home)
        log_path = os.path.join(home, "supervisor.log")
        env = os.environ.copy()
        env["OC_ORCH_HOME"] = home
        root = _module_root()
        if root:
            env["PYTHONPATH"] = root + (os.pathsep + env["PYTHONPATH"]
                                        if env.get("PYTHONPATH") else "")
        log = open(log_path, "a", encoding="utf-8")
        p = subprocess.Popen(
            [sys.executable, "-m", "oc_orch.cli", "--home", home,
             "supervisor", "run"],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log,
            start_new_session=True, env=env,
        )
        deadline = time.time() + 6
        st = supervisor_status(home, load_config(home))
        while time.time() < deadline and not st["healthy"]:
            time.sleep(0.5)
            st = supervisor_status(home, load_config(home))
        emit(args, {"ok": st["healthy"], "pid": p.pid, "supervisor": st,
                    "log": log_path},
             f"supervisor start pid={p.pid} healthy={st['healthy']} log={log_path}")
        return 0 if st["healthy"] else 1
    if args.action == "stop":
        st = supervisor_status(args.home, load_config(args.home))
        pid = st.get("pid")
        if not st["alive"] or not isinstance(pid, int):
            emit(args, {"ok": True, "already_stopped": True, "supervisor": st},
                 "supervisor not running")
            return 0
        os.kill(pid, signal.SIGTERM)
        deadline = time.time() + args.timeout
        while time.time() < deadline:
            time.sleep(0.3)
            st = supervisor_status(args.home, load_config(args.home))
            if not st["alive"]:
                break
        if st["alive"] and args.force:
            os.kill(pid, signal.SIGKILL)
            time.sleep(0.3)
        st = supervisor_status(args.home, load_config(args.home))
        emit(args, {"ok": not st["alive"], "supervisor": st},
             f"supervisor stopped={not st['alive']} pid={pid}")
        return 0 if not st["alive"] else 1
    if args.action == "run":
        sup = Supervisor(args.home, once=args.once)
        def _stop(signum, frame):
            sup.stop_event = True
        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)
        try:
            return sup.run()
        except SupervisorBusy as e:
            return fail(args, "supervisor_busy", str(e))
    return fail(args, "usage", "supervisor action 未实现")


# ---- argparse -----------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="oc-orch", description="OpenCode 多 session 可靠调谐")
    p.add_argument("--home", default=default_home(), help="状态目录（默认 XDG data）")
    p.add_argument("--json", action="store_true", help="结构化 JSON 输出")
    p.add_argument("--version", action="version", version=f"oc-orch {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("init", help="初始化台账")
    s.set_defaults(func=cmd_init)

    s = sub.add_parser("service", help="显示本机 OpenCode 服务")
    s.set_defaults(func=cmd_service)

    s = sub.add_parser("doctor", help="自检")
    s.add_argument("--probe-events", action="store_true")
    s.set_defaults(func=cmd_doctor)

    # task
    s = sub.add_parser("task", help="任务台账")
    tsub = s.add_subparsers(dest="task_action", required=True)
    c = tsub.add_parser("create")
    c.add_argument("--project", required=True)
    c.add_argument("--title", required=True)
    c.add_argument("--objective", default="")
    c.add_argument("--acceptance", action="append", default=[])
    c.add_argument("--acceptance-file")
    c.add_argument("--scope", action="append", default=[])
    c.add_argument("--deliverable", action="append", default=[])
    c.add_argument("--check", action="append", default=[])
    c.add_argument("--coordinator")
    c.add_argument("--priority", type=int, default=0)
    c.add_argument("--task-id")
    c.add_argument("--metadata")
    c.set_defaults(func=cmd_task_create)
    c = tsub.add_parser("list")
    c.add_argument("--status")
    c.add_argument("--project")
    c.set_defaults(func=cmd_task_list)
    c = tsub.add_parser("show")
    c.add_argument("task")
    c.set_defaults(func=cmd_task_show)
    c = tsub.add_parser("pause")
    c.add_argument("task")
    c.add_argument("--reason")
    c.set_defaults(func=cmd_task_pause)
    c = tsub.add_parser("resume")
    c.add_argument("task")
    c.set_defaults(func=cmd_task_resume)
    c = tsub.add_parser("cancel")
    c.add_argument("task")
    c.add_argument("--reason", default="manual-cancel")
    c.add_argument("--interrupt", action="store_true")
    c.set_defaults(func=cmd_task_cancel)

    # dispatch
    s = sub.add_parser("dispatch", help="创建/附着执行 session 并派单")
    s.add_argument("--task", required=True)
    s.add_argument("--session")
    s.add_argument("--new-session", action="store_true",
                   help="显式创建新 session（默认无 --session 时即创建）")
    s.add_argument("--directory")
    s.add_argument("--agent")
    s.add_argument("--model")
    s.add_argument("--variant")
    s.add_argument("--coordinator")
    s.add_argument("--prompt")
    s.add_argument("--prompt-file")
    s.add_argument("--deliverable", action="append", default=[])
    s.add_argument("--check", action="append", default=[])
    s.add_argument("--scope", action="append", default=[])
    s.add_argument("--deadline", type=int, help="相对期限（秒）")
    s.add_argument("--stall-after", type=int)
    s.add_argument("--allow-active", action="store_true")
    s.add_argument("--ignore-provider-backoff", action="store_true")
    s.add_argument("--no-prompt", action="store_true")
    s.add_argument("--dry-run", action="store_true")
    s.add_argument("--takeover-confirmed", action="store_true")
    s.add_argument("--collaboration", help="kind/reason/round JSON；额外分工说明关键路径收益，候选说明不确定性")
    s.add_argument("--work-mode", default="latency-first",
                   help="提示词工作模式：latency-first（默认）/budget-first/elastic；别名 fast/budget")
    s.set_defaults(func=cmd_dispatch)

    s = sub.add_parser("rework", help="对同一 session 发起新尝试")
    s.add_argument("--task", required=True)
    s.add_argument("--session")
    s.add_argument("--feedback")
    s.add_argument("--feedback-file")
    s.add_argument("--coordinator")
    s.add_argument("--deadline", type=int)
    s.add_argument("--stall-after", type=int)
    s.add_argument("--work-mode", default="latency-first",
                   help="提示词工作模式：latency-first（默认）/budget-first/elastic；别名 fast/budget")
    s.set_defaults(func=cmd_rework)

    # attempt
    s = sub.add_parser("attempt", help="执行尝试")
    asub = s.add_subparsers(dest="attempt_action", required=True)
    c = asub.add_parser("list")
    c.add_argument("--task")
    c.set_defaults(func=cmd_attempt_list)
    c = asub.add_parser("show")
    c.add_argument("attempt")
    c.set_defaults(func=cmd_attempt_show)
    c = asub.add_parser("stop")
    c.add_argument("attempt")
    c.add_argument("--interrupt", action="store_true")
    c.add_argument("--confirm", action="store_true")
    c.add_argument("--external-stopped", action="store_true")
    c.add_argument("--note")
    c.set_defaults(func=cmd_attempt_stop)
    c = asub.add_parser("release")
    c.add_argument("attempt")
    c.add_argument("--confirm", action="store_true")
    c.add_argument("--external-stopped", action="store_true")
    c.add_argument("--note")
    c.set_defaults(func=cmd_attempt_release)

    s = sub.add_parser("collect", help="收集结果与交付物")
    s.add_argument("--attempt", required=True)
    s.add_argument("--partial", action="store_true")
    s.add_argument("--carry-manifest", dest="carry_manifest",
                   help="显式沿用清单 JSON 路径（oc-orch.carry.v1）")
    s.set_defaults(func=cmd_collect)

    s = sub.add_parser("verify", help="验收")
    s.add_argument("--attempt", required=True)
    g = s.add_mutually_exclusive_group(required=True)
    g.add_argument("--pass", dest="pass_", action="store_true")
    g.add_argument("--fail", dest="pass_", action="store_false")
    s.add_argument("--evidence", action="append", default=[])
    s.add_argument("--notes")
    s.add_argument("--reason")
    s.add_argument("--run-checks", action="store_true")
    s.add_argument("--check-timeout", type=int, default=120)
    s.add_argument("--force", action="store_true")
    s.set_defaults(func=cmd_verify)

    s = sub.add_parser("status", help="结构化状态与摘要")
    s.add_argument("--live", action="store_true")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("health", help="本地只读业务健康（不同于Supervisor进程健康）")
    s.add_argument("--project")
    s.add_argument("--session")
    s.add_argument("--limit", type=int, default=50)
    s.add_argument("--after-id")
    s.set_defaults(func=cmd_health)

    s = sub.add_parser("delivery-policy", help="版本化交付策略（预设不产生资源/费用授权）")
    psub = s.add_subparsers(dest="action", required=True)
    c = psub.add_parser("show")
    target = c.add_mutually_exclusive_group(required=True)
    target.add_argument("--task")
    target.add_argument("--project")
    c.set_defaults(func=cmd_delivery_policy)
    for name, scope in (("set-project", "project"), ("switch-task", "task")):
        c = psub.add_parser(name)
        c.add_argument("--" + scope, required=True)
        c.add_argument("--policy", help="含preset/stage/limits的完整JSON对象")
        if scope == "task":
            c.add_argument("--inherit", action="store_true", help="冻结当前项目默认；不删除历史")
        c.add_argument("--expected-revision", type=int, required=True)
        c.add_argument("--operation-id", required=True)
        c.add_argument("--reason", required=True)
        c.add_argument("--confirm", required=True, help="逐字回填目标身份")
        c.set_defaults(func=cmd_delivery_policy)

    s = sub.add_parser("delivery-metrics", help="固定cohort/窗口事实导出的纯度量（不连服务/不写台账）")
    s.add_argument("--cohort", required=True)
    s.add_argument("--observations", required=True)
    s.add_argument("--as-of", type=int, help="可选窗口内事实截止毫秒；保留原窗口并标记interim，不改cohort")
    s.set_defaults(func=cmd_delivery_metrics)

    s = sub.add_parser("prototype", help="原型范围与权威缺口；记账不等于修复/验收")
    psub = s.add_subparsers(dest="action", required=True)
    for name in ("show", "update"):
        c = psub.add_parser(name)
        c.add_argument("--task", required=True)
        c.add_argument("--operation-id", required=name == "update")
        if name == "update":
            c.add_argument("--plan-file", required=True, help="原型计划JSON文件，仅记录/展示，不执行其中启动/检查文本")
            c.add_argument("--expected-revision", type=int, required=True)
            c.add_argument("--reason", required=True)
            c.add_argument("--confirm", required=True)
        c.set_defaults(func=cmd_prototype)

    s = sub.add_parser("conversation", help="显式用户补充指令与原ID结束对账（不自动接管/解暂停）")
    psub = s.add_subparsers(dest="action", required=True)
    for name in ("send", "show", "observe"):
        c = psub.add_parser(name)
        c.add_argument("--project", required=True)
        c.add_argument("--session", required=True)
        c.add_argument("--operation-id", required=True)
        if name == "send":
            c.add_argument("--text", required=True)
            c.add_argument("--reason", required=True)
            c.add_argument("--confirm", required=True)
            c.add_argument("--expected-revision", type=int, required=True, help="session控制版本，不是项目版本")
            c.add_argument("--expected-epoch", type=int, required=True)
        c.set_defaults(func=cmd_conversation)

    s = sub.add_parser("resource-authorization", help="独立资源授权；空resources显式撤销，模式不会自动补授")
    rsub = s.add_subparsers(dest="action", required=True)
    for name in ("show", "replace"):
        c = rsub.add_parser(name)
        c.add_argument("--scope", choices=("project", "task", "coordinator"), required=True)
        c.add_argument("--scope-id", required=True)
        if name == "replace":
            c.add_argument("--resources", required=True, help="严格资源JSON数组；无额外费用须带来源和内部路线覆盖声明")
            c.add_argument("--expires-at", type=int, required=True, help="绝对毫秒时间戳")
            c.add_argument("--expected-revision", type=int, required=True)
            c.add_argument("--operation-id", required=True)
            c.add_argument("--reason", required=True)
            c.add_argument("--confirm", required=True)
        c.set_defaults(func=cmd_resource_authorization)

    s = sub.add_parser("incidents", help="本地人工待办（只读；默认未配置邮件）")
    s.add_argument("--project")
    s.add_argument("--limit", type=int, default=50)
    s.add_argument("--after-id")
    s.set_defaults(func=cmd_incident)

    s = sub.add_parser("email", help="独立邮件配置/状态/单次tick（默认关闭）")
    es = s.add_subparsers(dest="action", required=True)
    es.add_parser("status", help="只读策略/outbox/executor状态").set_defaults(
        func=cmd_email)
    es.add_parser("check", help="校验本地 email-policy.json；不发送").set_defaults(
        func=cmd_email)
    es.add_parser("tick", help="执行一次有限发送/回执tick").set_defaults(
        func=cmd_email)

    s = sub.add_parser("reconcile", help="执行一次对账")
    s.add_argument("--dispatch", action="store_true", help="同时投递到期通知")
    s.set_defaults(func=cmd_reconcile)

    s = sub.add_parser("inbox", help="协调者读取/确认通知")
    s.add_argument("--session", required=True)
    s.add_argument("--ack-all", action="store_true")
    s.set_defaults(func=cmd_inbox)

    s = sub.add_parser("notify", help="通知管理")
    nsub = s.add_subparsers(dest="action", required=True)
    c = nsub.add_parser("list")
    c.add_argument("--state")
    c.set_defaults(func=cmd_notify)
    c = nsub.add_parser("ack")
    c.add_argument("ids", nargs="+")
    c.set_defaults(func=cmd_notify)
    c = nsub.add_parser("retry")
    c.add_argument("ids", nargs="+")
    c.set_defaults(func=cmd_notify)

    s = sub.add_parser("alerts", help="告警管理")
    asub2 = s.add_subparsers(dest="action", required=True)
    c = asub2.add_parser("list")
    c.add_argument("--all", action="store_true")
    c.set_defaults(func=cmd_alerts)
    c = asub2.add_parser("ack")
    c.add_argument("ids", nargs="+")
    c.set_defaults(func=cmd_alerts)

    s = sub.add_parser("coordinator", help="主协调租约与接管")
    csub = s.add_subparsers(dest="action", required=True)
    c = csub.add_parser("claim")
    c.add_argument("--project", required=True)
    c.add_argument("--session", required=True)
    c.add_argument("--takeover", action="store_true")
    c.add_argument("--confirm-old-stopped", action="store_true")
    c.add_argument("--ttl", type=int, default=300)
    c.set_defaults(func=cmd_coordinator)
    c = csub.add_parser("release")
    c.add_argument("--project", required=True)
    c.set_defaults(func=cmd_coordinator)
    c = csub.add_parser("status")
    c.set_defaults(func=cmd_coordinator)
    c = csub.add_parser("recover")
    c.add_argument("--project", required=True)
    c.add_argument("--session")
    c.add_argument("--message")
    c.set_defaults(func=cmd_coordinator)
    c = csub.add_parser("unlock", help="清除 manual 闸门（回合中断或显式暂停）")
    c.add_argument("--project", required=True)
    c.add_argument("--session")
    c.add_argument("--reason", default="manual-recovery")
    c.add_argument("--allow-provider", action="store_true",
                   help="显式允许对 provider 冷却/探测闸门执行恢复调度（operator "
                        "授权入口：复位为 probe-ready，新错误仍重新冷却；"
                        "manual 闸门始终可解锁，不依赖此开关）")
    c.set_defaults(func=cmd_coordinator)
    c = csub.add_parser("block", help="显式持久暂停协调者（需 unlock 恢复）")
    c.add_argument("--project", required=True)
    c.add_argument("--reason")
    c.set_defaults(func=cmd_coordinator)
    c = csub.add_parser("pause",
                        help="显式暂停项目自动调谐（共享用例；CAS/operation-id 必填）")
    c.add_argument("--project", required=True)
    c.add_argument("--session", required=True)
    c.add_argument("--operation-id", required=True,
                   help="原指令/操作身份引用")
    c.add_argument("--reason", required=True)
    c.add_argument("--expected-revision", type=int, required=True,
                   help="读到的当前控制版本（CAS）；先看 `coordinator control`")
    c.add_argument("--expected-epoch", type=int, help="当前租约代次（可选，传入即校验）")
    c.add_argument("--evidence", default=None,
                   help="证据 JSON 对象字符串（可选）；必须为对象，非对象按 usage 拒绝")
    c.set_defaults(func=cmd_coordinator)
    c = csub.add_parser("resume",
                        help="显式继续项目自动调谐（幂等；CAS/operation-id 必填）")
    c.add_argument("--project", required=True)
    c.add_argument("--session", required=True)
    c.add_argument("--operation-id", required=True)
    c.add_argument("--reason", required=True)
    c.add_argument("--expected-revision", type=int, required=True)
    c.add_argument("--expected-epoch", type=int)
    c.add_argument("--evidence", default=None,
                   help="证据 JSON 对象字符串（可选）；必须为对象，非对象按 usage 拒绝")
    c.set_defaults(func=cmd_coordinator)
    c = csub.add_parser("hold", help="回合级 hold（停止当前回答；不取消项目/worker）")
    c.add_argument("--session", required=True)
    c.add_argument("--event-id", required=True)
    c.add_argument("--origin-at", type=int, required=True,
                   help="回合中断稳定源时间（毫秒时间戳）")
    c.add_argument("--reason", required=True)
    c.set_defaults(func=cmd_coordinator)
    c = csub.add_parser("control", help="只读展示项目/会话控制与自动资格")
    c.add_argument("--project")
    c.add_argument("--session")
    c.set_defaults(func=cmd_coordinator)
    c = csub.add_parser("turn-begin",
                        help="建立用户回合屏障（仅显式可信判定；native hook 未接通）")
    c.add_argument("--session", required=True)
    c.add_argument("--message-id", required=True)
    c.add_argument("--origin-at", type=int, required=True)
    c.add_argument("--operation-id", required=True)
    c.add_argument("--trusted", action="store_true", required=True,
                   help="调用方显式判定可信 user 输入（本地断言；绝非 metadata 自称）")
    c.set_defaults(func=cmd_coordinator)
    c = csub.add_parser("turn-finish", help="以可信结束证据关闭用户回合（CAS 必填）")
    c.add_argument("--session", required=True)
    c.add_argument("--message-id", required=True, help="必须精确关联原输入")
    c.add_argument("--idle-id", required=True, help="回合结束 idle 证据 id")
    c.add_argument("--idle-at", type=int, required=True,
                   help="回合结束 idle 时间（毫秒时间戳）")
    c.add_argument("--expected-revision", type=int, required=True)
    c.set_defaults(func=cmd_coordinator)

    s = sub.add_parser("supervisor", help="监督进程")
    ssub = s.add_subparsers(dest="action", required=True)
    c = ssub.add_parser("run")
    c.add_argument("--once", action="store_true")
    c.set_defaults(func=cmd_supervisor)
    c = ssub.add_parser("start")
    c.set_defaults(func=cmd_supervisor)
    c = ssub.add_parser("stop")
    c.add_argument("--timeout", type=int, default=15)
    c.add_argument("--force", action="store_true")
    c.set_defaults(func=cmd_supervisor)
    c = ssub.add_parser("status")
    c.set_defaults(func=cmd_supervisor)

    s = sub.add_parser("send", help="向 session 发送幂等提示（默认登记托管责任）")
    s.add_argument("--session", required=True)
    s.add_argument("--text")
    s.add_argument("--file")
    s.add_argument("--message-id")
    s.add_argument("--delivery", choices=["queue", "steer"], default="queue")
    s.add_argument("--owner", help="发起责任人；托管发送必填（可推断时省略）")
    s.add_argument("--task", help="已托管任务归属（歧义时必须显式）")
    s.add_argument("--attempt", help="当前尝试归属（歧义时必须显式）")
    s.add_argument("--continuation-target",
                   help="执行结束后的接续目标 session（通常为协调者）")
    s.add_argument("--deadline", type=int,
                   help="责任处理期限（毫秒时间戳）；跨重启保存")
    s.add_argument("--material", action="append",
                   help="可定位材料引用（可重复）；只在责任证据中保存引用")
    s.add_argument("--work-kind", choices=["observe", "read_only", "write"],
                   default=None,
                   help="工作性质（显式声明）；缺省不猜只读：封存/释放目标会拒绝，"
                        "需改走 rework 或显式 --work-kind observe/read_only")
    s.add_argument("--same-candidate", action="store_true",
                   help="对同一构建和输入补充验法；需显式当前 --attempt，附逐条消费回执要求")
    s.add_argument("--untracked", action="store_true",
                   help="显式纯通信：不登记责任/不授予写权，并展示未托管边界")
    s.set_defaults(func=cmd_send)

    s = sub.add_parser("input", help="受管输入生命周期与白名单观察（S4）")
    isub = s.add_subparsers(dest="input_action", required=True)

    c = isub.add_parser("send", help="以显式意图发送受管输入")
    c.add_argument("--session", required=True)
    c.add_argument("--intent", choices=["execute", "park", "record"], required=True)
    c.add_argument("--owner", required=True)
    c.add_argument("--request-id", required=True)
    g = c.add_mutually_exclusive_group(required=True)
    g.add_argument("--text")
    g.add_argument("--file")
    c.add_argument("--project", default="")
    c.add_argument("--expires-at", type=int,
                   help="park 到期时间（毫秒整数）；到期只告警，不自动取消")
    c.add_argument("--flush-condition", help="park 的 flush 条件")
    c.add_argument("--task", help="托管任务归属（歧义时必须显式）")
    c.add_argument("--attempt", help="托管尝试归属")
    c.add_argument("--continuation-target",
                   help="执行结束后的接续目标 session（通常为协调者）")
    c.add_argument("--deadline", type=int,
                   help="责任处理期限（毫秒时间戳）；跨重启保存")
    c.add_argument("--material", action="append",
                   help="可定位材料引用（可重复）")
    c.add_argument("--work-kind", choices=["observe", "read_only", "write"],
                   default=None,
                   help="显式工作性质；缺省不猜只读（封存/释放目标会拒绝）")
    c.add_argument("--same-candidate", action="store_true",
                   help="对同一构建和输入补充验法；需显式当前 --attempt，附逐条消费回执要求")
    c.add_argument("--untracked", action="store_true",
                   help="显式纯通信：不登记任务责任/不授予写权并展示边界")
    c.set_defaults(func=cmd_input_send)

    c = isub.add_parser("list", help="列出受管输入（不含正文）")
    c.add_argument("--session")
    c.add_argument("--limit", type=int, default=100)
    c.set_defaults(func=cmd_input_list)

    c = isub.add_parser("show", help="查看单个输入回执与正文")
    c.add_argument("input")
    c.set_defaults(func=cmd_input_show)

    c = isub.add_parser("reconcile", help="对单个输入做有限对账")
    c.add_argument("input")
    c.set_defaults(func=cmd_input_reconcile)

    c = isub.add_parser("ack", help="显式 bool 业务 ACK（false 不 ack）")
    c.add_argument("input")
    c.add_argument("--value", choices=["true", "false"], required=True,
                   help="必须显式 true/false；false 不构成 ACK")
    c.add_argument("--owner", required=True)
    c.add_argument("--confirm", required=True, help="必须等于输入 ID")
    c.add_argument("--expected-revision", type=int, required=True)
    c.add_argument("--reason", required=True)
    c.set_defaults(func=cmd_input_ack)

    c = isub.add_parser("flush", help="原 ID flush；需对象确认/CAS/原因")
    c.add_argument("inputid")
    c.add_argument("--owner", required=True)
    c.add_argument("--confirm", required=True, help="必须等于 inputid")
    c.add_argument("--expected-revision", type=int, required=True)
    c.add_argument("--reason", required=True)
    c.set_defaults(func=cmd_input_flush)

    c = isub.add_parser("cancel", help="原 ID cancel；需对象确认/CAS/原因，不隐式 ACK")
    c.add_argument("inputid")
    c.add_argument("--owner", required=True)
    c.add_argument("--confirm", required=True, help="必须等于 inputid")
    c.add_argument("--expected-revision", type=int, required=True)
    c.add_argument("--reason", required=True)
    c.set_defaults(func=cmd_input_cancel)

    c = isub.add_parser("resume-target", help="人工闸门恢复（只解除人工控制）")
    c.add_argument("--session", required=True)
    c.add_argument("--owner", required=True)
    c.add_argument("--confirm", required=True, help="必须等于目标 session")
    c.add_argument("--expected-revision", type=int, required=True)
    c.add_argument("--reason", required=True)
    c.set_defaults(func=cmd_input_resume_target)

    c = isub.add_parser("watch", help="显式白名单观察分支")
    wsub = c.add_subparsers(dest="action", required=True)
    wc = wsub.add_parser("register")
    wc.add_argument("--session", required=True)
    wc.add_argument("--owner", required=True)
    wc.add_argument("--project", required=True)
    wc.add_argument("--max-pending-age", type=int, required=True,
                    help="pending 年龄阈值（秒），CLI 换算毫秒传给服务")
    wc.set_defaults(func=cmd_input_watch)
    wc = wsub.add_parser("unregister")
    wc.add_argument("--session", required=True)
    wc.add_argument("--owner", required=True)
    wc.add_argument("--confirm", required=True, help="必须等于 session")
    wc.add_argument("--expected-revision", type=int, required=True)
    wc.set_defaults(func=cmd_input_watch)
    wc = wsub.add_parser("list")
    wc.add_argument("--limit", type=int, default=100)
    wc.set_defaults(func=cmd_input_watch)
    wc = wsub.add_parser("observe")
    wc.add_argument("--limit", type=int, default=20)
    wc.set_defaults(func=cmd_input_watch)

    s = sub.add_parser("responsibility", help="后续责任：谁在等待什么（S4）")
    rsub = s.add_subparsers(dest="action", required=True)
    c = rsub.add_parser("list", help="列出未关闭责任")
    c.add_argument("--task")
    c.add_argument("--owner")
    c.add_argument("--limit", type=int, default=200)
    c.set_defaults(func=cmd_responsibility)
    c = rsub.add_parser("show", help="查看单个责任")
    c.add_argument("responsibility")
    c.set_defaults(func=cmd_responsibility)
    c = rsub.add_parser("transfer", help="转交责任（旧责任留痕 + 后继登记）")
    c.add_argument("responsibility")
    c.add_argument("--owner", required=True, help="新责任人")
    c.add_argument("--expected-revision", type=int, required=True)
    c.add_argument("--target", help="后继接续目标 session")
    c.add_argument("--state", default="awaiting_review",
                   choices=["handling", "waiting_input", "waiting_execution",
                            "waiting_dependency", "waiting_human",
                            "awaiting_review"])
    c.add_argument("--wait-kind", default="review")
    c.add_argument("--wait-condition", default="manual-transfer")
    c.add_argument("--next-action", default="核对交接材料与下一动作")
    c.add_argument("--condition-key", help="后继责任条件指纹（默认沿用原条件）")
    c.add_argument("--next-check-at", type=int)
    c.add_argument("--reason", default="manual-transfer")
    c.add_argument("--release-manual-confirm",
                   help="waiting_human 转出的人工确认（必须精确等于原责任ID）")
    c.set_defaults(func=cmd_responsibility)
    c = rsub.add_parser("resume", help="人工确认后离开 waiting_human（不授予写权）")
    c.add_argument("responsibility")
    c.add_argument("--confirm", required=True, help="必须等于责任 ID")
    c.add_argument("--expected-revision", type=int, required=True)
    c.add_argument("--reason", required=True)
    c.set_defaults(func=cmd_responsibility)
    c = rsub.add_parser("wait", help="登记等待依赖/人工/执行（不自动解除控制）")
    c.add_argument("responsibility")
    c.add_argument("--expected-revision", type=int, required=True)
    c.add_argument("--state", default="waiting_human",
                   choices=["handling", "waiting_input", "waiting_execution",
                            "waiting_dependency", "waiting_human",
                            "awaiting_review"])
    c.add_argument("--wait-kind", default="human")
    c.add_argument("--wait-condition", required=True)
    c.add_argument("--next-action")
    c.add_argument("--next-check-at", type=int)
    c.add_argument("--deadline", type=int)
    c.set_defaults(func=cmd_responsibility)
    c = rsub.add_parser("close", help="关闭推进责任（须业务终结/后继事实）")
    c.add_argument("responsibility")
    c.add_argument("--expected-revision", type=int, required=True)
    c.add_argument("--reason", required=True)
    c.add_argument("--superseded-by",
                   help="已有持久后继责任 ID；仍需 store 校验 stale/同任务事实")
    c.set_defaults(func=cmd_responsibility)

    s = sub.add_parser("permission", help="人工处理权限等待（绝不自动批准）")
    psub = s.add_subparsers(dest="action", required=True)
    c = psub.add_parser("list")
    c.add_argument("--directory", required=True)
    c.add_argument("--session")
    c.set_defaults(func=cmd_permission)
    c = psub.add_parser("reply")
    c.add_argument("--session", required=True)
    c.add_argument("--request", required=True)
    c.add_argument("--decision", choices=["once", "always", "reject"], required=True)
    c.add_argument("--message")
    c.set_defaults(func=cmd_permission)

    s = sub.add_parser("export", help="导出完整台账 JSON")
    s.add_argument("--out")
    s.set_defaults(func=cmd_export)

    s = sub.add_parser("web", help="本地 Web 控制面（默认 127.0.0.1）")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8787)
    s.add_argument("--read-only", action="store_true")
    s.add_argument("--daemon", action="store_true")
    s.add_argument("--stop", action="store_true")
    s.add_argument("--verbose", action="store_true")
    s.set_defaults(func=cmd_web)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    raw = list(sys.argv[1:] if argv is None else argv)
    # 允许 --json 出现在任意位置，便于在子命令后使用
    if "--json" in raw:
        raw = ["--json"] + [x for x in raw if x != "--json"]
    args = parser.parse_args(raw)
    if getattr(args, "home", None):
        os.environ["OC_ORCH_HOME"] = args.home
    try:
        return args.func(args)
    except ActionError as e:
        # 应用用例拒绝（含派单结果未知）；code→退出码映射见 EXIT。
        return fail(args, e.code, e.message, e.detail)
    except SupervisorBusy as e:
        return fail(args, "supervisor_busy", str(e))
    except LedgerError as e:
        code = "site_busy" if e.code == "SITE_BUSY" else "usage"
        return fail(args, code, str(e), e.detail)
    except TransportError as e:
        return fail(args, "service", str(e))
    except ApiError as e:
        code = "service" if e.status >= 500 else "usage"
        if e.status == 409:
            code = "usage"
        return fail(args, code, str(e), {"status": e.status, "tag": e.tag})
    except OrchError as e:
        return fail(args, "service", str(e))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())

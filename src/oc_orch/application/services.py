"""CLI/Web 共用用例。外部 I/O 在台账短事务之外，适配器由组合根注入。"""

from __future__ import annotations

from typing import Callable, Protocol

from ..domain.errors import ActionError, LedgerError, OrchError
from ..domain.prompts import build_prompt
from ..domain.work_mode import normalize_work_mode, render_work_mode
from .ledger_port import CommandLedger
from .ports import Clock, OpenCodeClient
from .delivery_action import authorize_attempt


class ResultPort(Protocol):
    def collect(self, attempt_id: str, *, partial: bool = False,
                carry_manifest: dict | None = None) -> dict: ...

    def scan_baseline(self, attempt_id: str) -> dict: ...

    def verify(self, attempt_id: str, **intent) -> dict: ...


class Orchestration:
    def __init__(self, ledger: CommandLedger, *, client: Callable[[], OpenCodeClient], results: ResultPort,
                 clock: Clock, new_id: Callable[[str, int], str]):
        self.ledger = ledger
        self._client = client
        self.results = results
        self.clock = clock
        self.new_id = new_id

    def ensure_site_free(self, att: dict) -> None:
        if att.get("site_released_at"):
            return
        if att["status"] not in (
            "terminal_succeeded", "terminal_failed", "terminal_interrupted",
            "result_collected", "result_missing", "verification_failed",
            "accepted", "rejected", "cancelled", "lost",
        ):
            raise LedgerError("SITE_BUSY", f"尝试 {att['id']} 仍处于 {att['status']}，不能释放现场")
        if (att.get("metadata") or {}).get("active_children"):
            raise LedgerError("SIDE_EFFECTS_RUNNING", "仍有活动子 session；需独立确认停止后释放现场")
        # 查询失败不能证明外部副作用已停止，保持写权直至显式确认。
        try:
            shells = self._client().shells(att["directory"])
        except OrchError as e:
            raise LedgerError("SITE_UNCONFIRMED", f"无法确认外部执行已停止：{e}") from e
        running = [s for s in shells if s.get("status") == "running" and (
            (s.get("metadata") or {}).get("sessionID") == att["session_id"] or
            ((s.get("metadata") or {}).get("sessionID") is None and s.get("cwd") == att["directory"]))]
        if running:
            raise LedgerError(
                "SIDE_EFFECTS_RUNNING",
                f"仍有 {len(running)} 个后台 shell；确认停止后用 "
                f"`oc-orch attempt release {att['id']} --confirm --external-stopped`",
                {"shells": [{"id": s.get("id"), "status": s.get("status")} for s in running]},
            )
        self.ledger.release_site(att["id"], confirmed=True, external_stopped=True,
                                 actor="coordinator", note="verified-no-running-shells")

    def _check_dispatch_control(self, attempt_id: str) -> None:
        try:
            self.ledger.check_attempt_dispatch(attempt_id)
        except LedgerError as exc:
            att = self.ledger.get_attempt(attempt_id)
            raise ActionError("gate", "派单期间任务已受人工控制；保留尝试与写权供核实",
                              {"attempt_id": attempt_id, "session_id": att["session_id"],
                               "reason": exc.code}) from exc

    # ---- 派单前执行边界封存（D1；2.2） ------------------------------
    def _snapshot_session_before(self, session_id: str | None) -> dict:
        """事务外读取派单前旧回合快照；失败/缺失保留可诊断空值，不冒充基线。"""
        if not session_id:
            return {}
        try:
            snap = self._client().get_session(session_id)
        except OrchError as e:
            raise ActionError(
                "gate", f"读取 session {session_id} 失败，不能封存旧回合基线：{e}",
                {"session_id": session_id}) from e
        if snap is None:
            return {"absent": True}
        times = snap.get("time") or {}

        def _t(key):
            v = times.get(key)
            return int(v) if isinstance(v, int) and v > 0 else None

        return {
            "outcome": snap.get("outcome"),
            "idle": _t("idle"), "busy": _t("busy"), "updated": _t("updated"),
            "snapshotted_at": self.clock.now_ms(),
        }

    def _scan_baseline_ref(self, att: dict) -> dict:
        """事务外扫描声明交付物基线（D5）；失败保留原因、不发送 prompt。"""
        try:
            return self.results.scan_baseline(att["id"])
        except Exception as e:
            raise ActionError(
                "gate", f"封存交付基线失败，不能发送 prompt：{e}",
                {"attempt_id": att["id"]}) from e

    def _seal_dispatch_baseline(self, att: dict, *, mode: str) -> dict:
        """派单可能发送前封存执行边界；失败保留尝试、不发送 prompt（D1）。

        返回实际冻结的 session_before（attach 模式下供附着 epoch 使用）。
        """
        if mode == "new":
            session_before: dict = {}  # 确定的新建身份：无旧回合来自身份，不来自 GET
        elif mode == "attach":
            session_before = self._snapshot_session_before(att["session_id"])
            if session_before.get("absent"):
                raise ActionError("gate",
                                  f"附着目标 session 不存在：{att['session_id']}",
                                  {"attempt_id": att["id"]})
        else:  # reuse：复用 session 的旧 outcome/idle/busy 一律冻结为基线
            session_before = self._snapshot_session_before(att["session_id"])
            if session_before.get("absent"):
                raise ActionError(
                    "gate", f"复用 session 不存在：{att['session_id']}",
                    {"attempt_id": att["id"]}) from None
        # 交付基线只扫描声明目标；command/note 不在派单前执行（D5）
        baseline_ref = self._scan_baseline_ref(att)
        self.ledger.seal_execution(
            att["id"], mode=mode, session_before=session_before,
            baseline=baseline_ref, allow_send=True,
            actor="coordinator", reason=f"dispatch-{mode}",
        )
        return session_before

    def _record_attach_epoch(self, att: dict, session_before: dict | None) -> None:
        """F10/T15：no_prompt 附着时仅捕获**正在运行的 busy 区间**为执行开始。

        附着不伪造派单消息；epoch 使该区间的后续 idle 能经统一归属规则确认。
        历史已完成 busy（有更晚 idle，idle > busy）不能当新 epoch；无 busy 快照
        则等待新的执行事实，不消费过去的成功。
        """
        before = session_before or {}
        busy = before.get("busy")
        if not isinstance(busy, int) or busy <= 0:
            return
        idle = before.get("idle")
        if isinstance(idle, int) and idle > busy:
            return  # 历史已完成 busy：不当新执行开始
        execution = (att.get("metadata") or {}).get("execution") or {}
        if (execution.get("epoch") or {}).get("started_at"):
            return
        self.ledger.record_execution_started(
            att["id"], started_at=busy, event_id=None, source="attach-busy",
            actor="coordinator")

    def dispatch(self, task_id: str, *, directory: str, coordinator: str | None,
                 session: str | None = None, model: str | None = None,
                 agent: str | None = None, variant: str | None = None,
                 scope: list[str] = (), deliverables: list[str] = (),
                 checks: list[str] = (), prompt: str | None = None,
                 allow_active: bool = False, ignore_provider_backoff: bool = False,
                 dry_run: bool = False, no_prompt: bool = False,
                  deadline: int | None = None, stall_after: int | None = None,
                  takeover_confirmed: bool = False, collaboration: dict | None = None,
                  work_mode: str = "latency-first") -> dict:
        work_mode = normalize_work_mode(work_mode)
        if collaboration is not None:
            from ..domain.delivery_action import collaboration as collaboration_value

            collaboration = collaboration_value(collaboration)
        task = self.ledger.get_task(task_id)
        if task["status"] in ("paused", "cancelled", "accepted"):
            raise ActionError("usage", f"任务处于 {task['status']}，不能派单")
        self.ledger.check_project_dispatch(task["project"])
        cur = task.get("current_attempt_id")
        if cur and not allow_active:
            existing = self.ledger.get_attempt(cur)
            if existing["status"] not in (
                "terminal_succeeded", "terminal_failed", "terminal_interrupted",
                "result_collected", "result_missing", "verification_failed",
                "rejected", "cancelled", "lost",
            ):
                raise ActionError("site_busy", f"任务已有未结束尝试 {cur}（{existing['status']}）；"
                                  "先完成/停止并释放现场，或显式 --allow-active",
                                  {"attempt": cur, "status": existing["status"]})
        coordinator = coordinator or task.get("coordinator_session")
        if not coordinator:
            lease = self.ledger.coordinator_lease(task["project"])
            coordinator = lease["holder_session"] if lease else None
        if not coordinator:
            raise ActionError("usage", "缺少主协调 session：用 --coordinator、任务登记或 coordinator claim 指定")
        authority = self.ledger.check_dispatch_authority(task["project"], coordinator)
        if model and "/" not in model:
            raise LedgerError("INVALID_MODEL", "模型格式应为 provider/model")
        model_spec = dict(zip(("providerID", "id"), model.split("/", 1))) if model else None
        if model and not ignore_provider_backoff:
            state = self.ledger.provider_states().get(model) or {}
            if state.get("until") and int(state["until"]) > self.clock.now_ms():
                raise ActionError("provider_backoff", f"provider {model} 处于退避/阻塞（{state.get('category')}），"
                                  "不得通过扩 session 放大限流；确需派单用 --ignore-provider-backoff", state)
        if session:
            existing_session = self._client().get_session(session)
            if existing_session is None:
                raise ActionError("usage", f"session 不存在：{session}")
            directory = (existing_session.get("location") or {}).get("directory") or directory
        scope = list(scope or task.get("write_scope") or [directory])
        declared = (task.get("metadata") or {}).get("deliverables") or []
        declared = [d if isinstance(d, str) else str(d["target"])
                    for d in declared if isinstance(d, str) or isinstance(d, dict) and d.get("target")]
        deliverables = list(dict.fromkeys([*declared, *deliverables]))
        checks = list(dict.fromkeys([*((task.get("metadata") or {}).get("checks") or []), *checks]))
        att_id = self.new_id("A-", 12)
        session_id = session or ("ses_ocorch" + self.new_id("", 16))
        # T15：no_prompt 附着不伪造派单消息；仅真实 prompt 才有 dispatch message
        msg_id = ("msg_ocorch" + self.new_id("", 16)) if not no_prompt else None
        generation = (self.ledger.get_attempt(cur)["generation"] + 1) if cur else 1
        create_spec = {
            "session_id": session_id, "directory": directory,
            "title": f"{task['id']} {task['title']}"[:120], "agent": agent,
            "model": ({**model_spec, "variant": variant} if model_spec and variant else model_spec),
            "metadata": {"oc_orch_task": task["id"], "oc_orch_attempt": att_id,
                         "oc_orch_project": task["project"], "oc_orch_generation": generation},
        }
        text = build_prompt(task, att_id, generation, directory, scope,
                            deliverables + [f"check:{c}" for c in checks], prompt,
                            work_mode=work_mode)
        if dry_run:
            return {"ok": True, "dry_run": True, "task": task["id"], "attempt_id": att_id,
                    "session_id": session_id, "prompt": text, "work_mode": work_mode,
                    "create": create_spec}
        def prepare_metadata(assigned_generation):
            # 纯值构造在认领事务内完成；崩溃后 prepared 本身足以按原 ID 恢复。
            create = {**create_spec, "metadata": {**create_spec["metadata"],
                                                 "oc_orch_generation": assigned_generation}}
            text = build_prompt(task, att_id, assigned_generation, directory, scope,
                                deliverables + [f"check:{c}" for c in checks], prompt,
                                work_mode=work_mode)
            return {"dispatch": {"session_id_planned": session_id, "prompt": text, "create": create,
                                  "no_prompt": no_prompt, "work_mode": work_mode},
                    **({"delivery_collaboration": collaboration} if collaboration is not None else {}),
                    "coordinator": coordinator, "deliverables": deliverables, "checks": checks}

        att = self.ledger.create_attempt(
            task["id"], attempt_id=att_id, directory=directory, session_id=session_id,
            agent=agent, model=model, variant=variant, status="creating",
            dispatch_state="prepared", dispatch_msg_id=msg_id,
            deadline_at=(self.clock.now_ms() + deadline * 1000 if deadline else None),
            stall_after=stall_after,
            deliverables=deliverables + [{"kind": "command", "target": c} for c in checks],
            write_scope=scope, prepare_metadata=prepare_metadata,
            takeover_confirmed=takeover_confirmed,
            coordinator=coordinator, dispatch_authority=authority,
            **({"expected_current": cur} if not allow_active else {}),
        )
        # generation 以认领事务的结果为准，而非事务外预测值。
        generation = att["generation"]
        create_spec = att["metadata"]["dispatch"]["create"]
        text = att["metadata"]["dispatch"]["prompt"]
        # 派单前封存执行边界（D1）：任何 POST 之前完成；失败不发 prompt。
        mode = "attach" if no_prompt else ("reuse" if session else "new")
        seal_before = self._seal_dispatch_baseline(att, mode=mode)
        attach_before = seal_before if mode == "attach" else None
        att = self.ledger.get_attempt(att_id)
        try:
            self._check_dispatch_control(att_id)
            if not session:
                self._client().create_session(**create_spec)
            self.ledger.record_dispatch(att_id, "session_created", reason="session-created")
            self._check_dispatch_control(att_id)
            if not no_prompt:
                authorize_attempt(self.ledger, self._client(), self.clock, att_id)
                self._client().prompt(session_id, message_id=msg_id, text=text, delivery="queue",
                                      metadata={"oc_orch_task": task_id, "oc_orch_attempt": att_id,
                                                "oc_orch_generation": generation})
            self.ledger.record_dispatch(att_id, "confirmed", started=not no_prompt,
                                        task_status="dispatched",
                                        reason="dispatch-confirmed" if not no_prompt else "attach-existing-no-prompt")
            if no_prompt and mode == "attach":
                # F10/T15：附着 busy 区间冻结为可归属开始（不伪造派单消息）
                self._record_attach_epoch(
                    self.ledger.get_attempt(att_id), attach_before)
        except OrchError as e:
            self.ledger.record_dispatch(att_id, "unknown", reason=f"dispatch-error:{e.code}")
            raise ActionError("dispatch_unknown", f"派单结果未知，已保留尝试 {att_id} 供对账（不会换 ID 重投）：{e}",
                              {"attempt_id": att_id, "session_id": session_id, "message_id": msg_id}) from e
        return {"ok": True, "task_id": task_id, "attempt_id": att_id, "session_id": session_id,
                "message_id": msg_id, "directory": directory, "prompt_chars": len(text)}

    def rework(self, task_id: str, *, feedback: str, session: str | None = None,
               coordinator: str | None = None, deadline: int | None = None,
               stall_after: int | None = None,
               work_mode: str = "latency-first") -> dict:
        task = self.ledger.get_task(task_id)
        self.ledger.check_project_dispatch(task["project"])
        if task["status"] in ("paused", "cancelled", "accepted"):
            raise ActionError("usage", f"任务处于 {task['status']}，不能返工")
        coordinator = coordinator or task.get("coordinator_session")
        authority = self.ledger.check_dispatch_authority(task["project"], coordinator)
        old = self.ledger.get_attempt(task["current_attempt_id"]) if task.get("current_attempt_id") else None
        if old is None:
            raise ActionError("usage", "任务没有当前尝试")
        if old["status"] not in ("terminal_succeeded", "terminal_failed", "terminal_interrupted",
                                 "result_collected", "result_missing", "verification_failed", "rejected"):
            raise ActionError("usage", f"当前尝试 {old['id']} 状态 {old['status']} 不能返工")
        if not feedback:
            raise ActionError("usage", "返工需要 --feedback 或 --feedback-file")
        self.ensure_site_free(old)
        generation = old["generation"] + 1
        session_id = session or old["session_id"]
        if not session_id:
            raise ActionError("usage", "旧尝试没有 session；请用 --new-session 走 dispatch")
        att_id, msg_id = self.new_id("A-", 12), "msg_ocorch" + self.new_id("", 16)
        work_mode = normalize_work_mode(work_mode)
        text = (f"返工/继续通知（任务 {task_id} / 新尝试 {att_id} / 第 {generation} 次尝试）。\n"
                f"上一尝试：{old['id']}（{old['status']}）。\n验收反馈：\n{feedback.strip()}\n\n"
                + render_work_mode(work_mode) + "\n\n"
                "请优先利用原 session 上下文修复问题；不要重做已验收部分。\n"
                "完成后按原回报格式提交，并保留任务/尝试标识。\n")
        att = self.ledger.create_attempt(
            task_id, attempt_id=att_id, directory=old["directory"], session_id=session_id,
            agent=old.get("agent"), model=old.get("model"), variant=old.get("variant"),
            generation=generation, status="creating", dispatch_state="prepared", dispatch_msg_id=msg_id,
            deadline_at=(self.clock.now_ms() + deadline * 1000 if deadline else old.get("deadline_at")),
            stall_after=stall_after or old.get("stall_after"),
            deliverables=[{"kind": d["kind"], "target": d["target"], "required": bool(d["required"])}
                          for d in self.ledger.deliverables(old["id"])],
            write_scope=old["metadata"].get("write_scope") or [old["directory"]],
            metadata={"dispatch": {"session_id_planned": session_id, "prompt": text, "create": {},
                                    "work_mode": work_mode},
                      "coordinator": coordinator or task.get("coordinator_session"), "rework_of": old["id"]},
            expected_current=old["id"],
            coordinator=coordinator, dispatch_authority=authority,
        )
        # 派单前封存执行边界：复用 session 的旧 outcome/idle/busy 冻结为基线（D1）
        self._seal_dispatch_baseline(att, mode="reuse")
        att = self.ledger.get_attempt(att_id)
        try:
            self._check_dispatch_control(att_id)
            authorize_attempt(self.ledger, self._client(), self.clock, att_id)
            text = att["metadata"]["dispatch"]["prompt"]
            self._client().prompt(session_id, message_id=msg_id, text=text, delivery="queue",
                                  metadata={"oc_orch_task": task_id, "oc_orch_attempt": att_id,
                                            "oc_orch_generation": generation, "oc_orch_rework_of": old["id"]})
            self.ledger.record_dispatch(att_id, "confirmed", started=True,
                                        task_status="running", reason="rework-confirmed")
        except OrchError as e:
            self.ledger.record_dispatch(att_id, "unknown", reason=f"rework-error:{e.code}")
            raise ActionError("dispatch_unknown", f"返工派单结果未知，保留尝试 {att_id}：{e}",
                              {"attempt_id": att_id, "session_id": session_id}) from e
        return {"ok": True, "task_id": task_id, "attempt_id": att_id, "session_id": session_id,
                "generation": att["generation"], "message_id": msg_id}

    def verify(self, attempt_id: str, *, passed: bool, **intent) -> dict:
        try:
            outcome = self.results.verify(attempt_id, result="pass" if passed else "fail", **intent)
        except LedgerError as e:
            code = "usage" if e.code in {"REASON_REQUIRED", "FORCE_REASON_REQUIRED", "INVALID_RESULT"} else "gate"
            raise ActionError(code, str(e), e.detail) from e
        task, att = outcome["task"], outcome["attempt"]
        if passed and not outcome["already"] and att.get("site_released_at") is None:
            try:
                self.ensure_site_free(att)
            except LedgerError:
                pass
            att = self.ledger.get_attempt(attempt_id)
        if not outcome["already"]:
            self.ledger.ack_notifications_for_task(task["id"], actor="coordinator:verify")
        return {"ok": True, "task": task, "attempt": att, "verification": outcome["verification"] or {},
                "changed": outcome["changed"], "already": outcome["already"],
                "already_accepted": bool(outcome["already"] and passed)}

    def collect(self, attempt_id: str, *, partial: bool = False,
                carry_manifest: dict | None = None) -> dict:
        """人工收集（D7/D9）：经共享用例；不隐式 ACK，返回版本与通知 ID。"""
        att = self.ledger.get_attempt(attempt_id)
        if att["status"] == "accepted":
            return {"ok": True, "attempt_id": attempt_id, "status": "accepted",
                    "already_accepted": True, "result_path": att.get("result_path"),
                    "result_version": ((att["metadata"] or {}).get("result") or {})
                    .get("version"), "notification_ids": [], "partial": False}
        try:
            info = self.results.collect(attempt_id, partial=partial,
                                        carry_manifest=carry_manifest)
        except LedgerError as e:
            # carry 输入/来源错误保留稳定 code（D6）；一般输入错误仍按 usage
            code = (e.code if e.code.startswith("CARRY_") else
                    ("usage" if e.code in {"REASON_REQUIRED", "FORCE_REASON_REQUIRED",
                                           "INVALID_RESULT"} else "gate"))
            raise ActionError(code, str(e), e.detail) from e
        return {
            "ok": True, "attempt_id": attempt_id, "status": info["status"],
            "partial": bool(info.get("partial")),
            "diagnostic_path": info.get("diagnostic_path"),
            "result_path": info.get("result_path"),
            "result_version": info.get("result_version"),
            "notification_ids": info.get("notification_ids") or [],
            "classification": info.get("classification"),
            "collection_state": info.get("collection_state"),
            "ready_for_verification": info.get("ready_for_verification"),
            "next_collect_at": info.get("next_collect_at"),
            "pending": bool(info.get("pending")),
            "duplicate": bool(info.get("duplicate")),
            "effective": bool(info.get("effective")),
            "text_chars": info.get("text_chars", 0),
            "assistant_message_count": info.get("assistant_message_count", 0),
            "missing": [d["target"] for d in info.get("missing") or []],
            "unverified": [d["target"] for d in info.get("unverified") or []],
            "stale": [d["target"] for d in info.get("stale") or []],
            "deliverables": [{"target": d["target"], "kind": d["kind"],
                              "status": d["status"],
                              "provenance": d.get("provenance")}
                             for d in info.get("deliverables") or []],
        }

    def cancel(self, task_id: str, *, reason: str = "", interrupt: bool = False) -> dict:
        task = self.ledger.get_task(task_id)
        # 人工取消先落库，防 interrupt 期间崩溃或并发派单丢失取消意图。
        if task["status"] != "cancelled":
            task = self.ledger.transition_task(task_id, "cancelled", reason=reason, actor="coordinator",
                                               extra={"closed_at": self.clock.now_ms()})
        if interrupt:
            attempts = self.ledger.open_attempts(task_id=task_id)
            for att in attempts:
                self.ledger.stop_attempt(att["id"], actor="coordinator")
            for att in attempts:
                if att["session_id"]:
                    try:
                        self._client().interrupt(att["session_id"])
                    except OrchError:
                        pass  # 停止结果未知；意图已持久化且不释放写权。
        return {"ok": True, "task": task}

    def pause(self, task_id: str, *, reason: str = "manual-pause") -> dict:
        task = self.ledger.get_task(task_id)
        if task["status"] != "paused":
            meta = {**task["metadata"], "paused_from": task["status"]}
            task = self.ledger.transition_task(task_id, "paused", reason=reason, actor="coordinator", extra={"metadata": meta})
        return {"ok": True, "task": task}

    def resume(self, task_id: str) -> dict:
        task = self.ledger.get_task(task_id)
        if task["status"] != "paused":
            raise ActionError("usage", f"{task_id} 不是 paused")
        back = task["metadata"].get("paused_from") or "running"
        if back not in ("draft", "dispatched", "running", "blocked", "stalled", "result_pending", "result_received", "rejected"):
            back = "running"
        return {"ok": True, "task": self.ledger.transition_task(task_id, back, reason="manual-resume", actor="coordinator")}

    def ack_notifications(self, ids: list[str]) -> dict:
        return {"ok": True, "acked": self.ledger.ack_notifications(ids, actor="coordinator")}

    def ack_alerts(self, ids: list[str]) -> dict:
        return {"ok": True, "alerts": [self.ledger.ack_alert(i, actor="coordinator") for i in ids]}

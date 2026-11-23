#!/usr/bin/env python3
"""离线集成测试共享 fake：可推进时钟 + 内存版 OpenCode V2 服务模型。

不读取个人服务配置、不发起真实网络；与真临时 SQLite 台账配合，使
observe/reconcile/通知等恢复剧本可确定性复现（受控时间驱动，无概率 sleep）。

接口对齐 oc_orch.application.ports.Clock / OpenCodeClient；Supervisor 与
application 用例只依赖这些窄端口。方法名列一律不与实例属性重名
（消息用 _messages_store、表单用 _forms_store、shell 用 _shells_store）。
"""

from __future__ import annotations

import threading
from typing import Any, Iterator

from oc_orch.client import ApiError, TransportError


class FakeClock:
    """线程安全、可推进的 wall/monotonic；sleep 记账并按需推进时间。

    now_ms() 用于持久 deadline，monotonic() 用于单进程间隔；advance 默认
    同时推进两者（真实时钟两者同步）。
    """

    def __init__(self, wall_ms: int = 1_000_000, mono_ms: int = 0):
        self._wall = int(wall_ms)
        self._mono = int(mono_ms)
        self._lock = threading.Lock()
        self.sleeps: list[float] = []

    def now_ms(self) -> int:
        with self._lock:
            return self._wall

    def monotonic(self) -> int:
        with self._lock:
            return self._mono

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self.sleeps.append(seconds)
            step = int(seconds * 1000)
            self._wall += step
            self._mono += step

    def advance(self, *, wall: int | None = None, mono: int | None = None,
                seconds: float | None = None) -> None:
        if seconds is not None:
            step = int(seconds * 1000)
            wall = mono = step
        with self._lock:
            if wall is not None:
                self._wall += wall
            if wall is not None and mono is None:
                mono = wall
            if mono is not None:
                self._mono += mono


class FakeService:
    def __init__(self, base: str = "http://127.0.0.1:0",
                 host: str = "127.0.0.1", port: int = 0):
        self.base = base
        self.host = host
        self.port = port

    def public(self) -> dict:
        return {"base": self.base, "host": self.host, "port": self.port,
                "scheme": "http", "config": ""}


class FakeClient:
    """内存版 OpenCode V2 服务；方法名/签名对齐 OpenCodeClient 窄端口。

    - sessions/inbox/messages/forms/shells/active 均为内存态，可预先布置或
      在调用后断言；prompt() 只把消息排入 inbox（V2 queue 语义），消息要
      经 deliver_inbox() 模拟服务端投递后才可见（与真实服务一致）。
    - 消息形状对齐固封 schema（evidence/2026-11-08-completion-integrity-review/
      message-protocol.json）：user 消息正文是 ``text``（字符串）；inbox 条目
      含 ``payload``/``delivery``；idle 消息 ``type=idle`` + ``outcome``。
      为兼容既有夹具，读取侧对旧 ``texts`` 字段保持回退。
    - 消息按 (time.created, id) 排序；message_page 支持 asc/desc 首页与同向
      cursor 翻页；坏 cursor 按真实服务语义抛 400 ApiError，不静默空页。
    - 通过 fail(method, exc[, times]) 按方法注入异常队列：每次调用弹出并抛出
      一个，确定性触发 429/403/409/404/TransportError 等分支；exc=None 忽略
      （防御历史调用把 None 入队后在 _maybe_fail 处 raise None 崩溃）。
    - prompt_conflict_admits=True 时，prompt 在抛 409 前先把消息写回消息存储，
      模拟“冲突但已受理”——用于 reconcile 409 后的重查确认分支。
    """

    def __init__(self, service: FakeService | None = None):
        self.service = service or FakeService()
        self.sessions: dict[str, dict] = {}
        self.inboxes: dict[str, list[dict]] = {}
        self._messages_store: dict[str, list[dict]] = {}
        self.permission_requests: list[dict] = []
        self._forms_store: dict[str, list[dict]] = {}
        self._shells_store: dict[str, list[dict]] = {}
        self.children: dict[str, list[dict]] = {}
        self.active: dict[str, dict] = {}
        self.prompt_calls: list[dict] = []
        self.create_calls: list[dict] = []
        self.switch_calls: list[dict] = []
        self.interrupt_calls: list[dict] = []
        self._failures: dict[str, list[Exception]] = {}
        self.info_payload: dict = {"version": "0.0.0-test"}
        self.prompt_conflict_admits: bool = False
        # 调度与投递分别模拟；已有测试仍可显式 deliver_inbox，不隐式推进模型。
        self.scheduled_sessions: set[str] = set()
        self.inbox_lifecycle_events: list[dict] = []
        # 消息历史可见上限：模拟真实服务分页/截断导致“翻不到更早锚点”
        # （coverage 不足）；None 表示完整历史可见。
        self.message_scan_cap: int | None = None

    # ---- 错误注入（确定性，非概率） -------------------------------
    def fail(self, method: str, exc: Exception | None, *, times: int = 1) -> None:
        """注入异常队列；exc=None 不入队（防 raise None 的 TypeError）。"""
        if exc is None:
            return
        self._failures.setdefault(method, []).extend([exc] * times)

    def clear_failures(self, method: str | None = None) -> None:
        if method is None:
            self._failures.clear()
        else:
            self._failures.pop(method, None)

    def _maybe_fail(self, method: str) -> None:
        q = self._failures.get(method)
        if q:
            raise q.pop(0)

    # ---- 布置 -----------------------------------------------------
    def set_session(self, sid: str, *, outcome: str | None = None,
                    time_: dict | None = None, active: bool = False,
                    location_dir: str | None = None, model: dict | None = None,
                    title: str = "") -> dict:
        session = self.sessions.setdefault(sid, {
            "id": sid, "outcome": None, "agent": "Fake", "model": None,
            "title": title, "time": {}, "location": {},
        })
        session["outcome"] = outcome
        if time_ is not None:
            session["time"] = time_
        if location_dir is not None:
            session["location"] = {"directory": location_dir}
        if model is not None:
            session["model"] = model
        if active:
            self.active[sid] = session
        return dict(session)

    def set_active(self, sids: list[str]) -> None:
        for sid in sids:
            if sid in self.sessions:
                self.active[sid] = self.sessions[sid]

    def clear_active(self) -> None:
        self.active.clear()

    def set_inbox(self, sid: str, items: list[dict]) -> None:
        self.inboxes[sid] = list(items)

    def append_message(self, sid: str, msg: dict) -> None:
        self._messages_store.setdefault(sid, []).append(msg)

    def _replace_message(self, sid: str, msg: dict) -> None:
        store = self._messages_store.setdefault(sid, [])
        store[:] = [m for m in store if m.get("id") != msg.get("id")]
        store.append(msg)

    def deliver_inbox(self, sid: str, *, at: int) -> list[dict]:
        """模拟服务端投递：inbox 中排队消息移出队列并替换为带 created 的 user 消息。

        保留原 message id，使“原输入已执行”锚点可按 ID + created 时间核验。
        投递后的消息使用固封 V2 形状（Session.Message.User：正文在 ``text``）；
        对旧夹具手写的 ``texts``/顶层 ``text`` inbox 条目保持读取回退。
        """
        out: list[dict] = []
        for m in self.inboxes.pop(sid, []):
            payload = m.get("payload") if isinstance(m.get("payload"), dict) else {}
            text = m.get("texts") or m.get("text") or payload.get("text") or ""
            msg = {"id": m.get("id"), "type": "user", "text": text,
                   "time": {"created": at}}
            metadata = m.get("metadata") or payload.get("metadata")
            if isinstance(metadata, dict) and metadata:
                msg["metadata"] = metadata
            self._replace_message(sid, msg)
            self.inbox_lifecycle_events.append({
                "type": "session.inbox.delivered", "created": at,
                "data": {"sessionID": sid, "inboxID": m.get("id")},
            })
            out.append(msg)
        return out

    def run_scheduled(self, sid: str, *, at: int) -> list[dict]:
        """显式推进已排执行的会话；park 本身不能排执行，投递保持 FIFO。"""
        if sid not in self.scheduled_sessions:
            return []
        self.scheduled_sessions.remove(sid)
        if sid in self.sessions:
            self.active[sid] = self.sessions[sid]
        self.inbox_lifecycle_events.append({
            "type": "session.execution.started", "created": at,
            "data": {"sessionID": sid},
        })
        return self.deliver_inbox(sid, at=at)

    def update_inbox_delivery(self, session_id: str, inbox_id: str, *,
                              delivery: str = "steer") -> None:
        self._maybe_fail("update_inbox_delivery")
        if delivery not in ("queue", "steer"):
            raise ValueError("delivery 必须为 queue 或 steer")
        for item in self.inboxes.get(session_id, []):
            if item.get("id") == inbox_id:
                item["delivery"] = delivery
                if delivery == "steer":
                    self.scheduled_sessions.add(session_id)
                return
        raise ApiError(409, "/inbox", "ConflictError", "input unavailable")

    def cancel_inbox(self, session_id: str, inbox_id: str) -> None:
        self._maybe_fail("cancel_inbox")
        items = self.inboxes.get(session_id, [])
        remaining = [item for item in items if item.get("id") != inbox_id]
        if len(remaining) != len(items):
            self.inboxes[session_id] = remaining
            self.inbox_lifecycle_events.append({
                "type": "session.inbox.cancelled",
                "data": {"sessionID": session_id, "inboxID": inbox_id},
            })

    def add_idle(self, sid: str, *, outcome: str, at: int, msg_id: str | None = None,
                 error: dict | None = None) -> dict:
        """布置 V2 idle 消息（Session.Message.Idle：type=idle + outcome 必填）。"""
        msg = {"id": msg_id or f"msg_idle_{at}_{len(self._messages_store.get(sid, []))}",
               "type": "idle", "time": {"created": at}, "outcome": outcome}
        if error is not None:
            msg["error"] = error
        self.append_message(sid, msg)
        return dict(msg)

    def _visible_messages(self, sid: str) -> list[dict]:
        """按 message_scan_cap 截断可翻页历史，模拟查询受限导致的覆盖不足。"""
        msgs = list(self._messages_store.get(sid, []))

        def key(m: dict) -> tuple[int, str]:
            t = m.get("time") or {}
            return (int(t.get("created") or 0), str(m.get("id") or ""))

        msgs.sort(key=key)
        if self.message_scan_cap is not None:
            msgs = msgs[-self.message_scan_cap:]
        return msgs

    def add_permission(self, *, directory: str, session_id: str, pid: str = "per_1",
                       action: str = "shell", resources: list[str] | None = None,
                       save: bool = False) -> dict:
        req = {"id": pid, "sessionID": session_id, "directory": directory,
               "action": action, "resources": resources or [], "save": save}
        self.permission_requests.append(req)
        return dict(req)

    def set_forms(self, sid: str, items: list[dict]) -> None:
        self._forms_store[sid] = list(items)

    def add_shell(self, *, directory: str, shell_id: str = "sh_1",
                  status: str = "running", metadata: dict | None = None,
                  cwd: str | None = None) -> dict:
        sh = {"id": shell_id, "status": status, "metadata": metadata or {},
              "cwd": cwd or directory}
        self._shells_store.setdefault(directory, []).append(sh)
        return dict(sh)

    # ---- OpenCodeClient 端口实现 ----------------------------------
    def reload_service(self) -> dict:
        self._maybe_fail("reload_service")
        return {"changed": False, "base": self.service.base}

    def iter_events(self, types: list[str], *, socket_timeout: float = 90.0
                    ) -> Iterator[dict]:
        return iter([])

    def info(self) -> dict:
        self._maybe_fail("info")
        return dict(self.info_payload)

    def active_sessions(self) -> dict[str, dict]:
        self._maybe_fail("active_sessions")
        return dict(self.active)

    def get_session(self, session_id: str) -> dict | None:
        self._maybe_fail("get_session")
        return self.sessions.get(session_id)

    def list_sessions(self, *, parent_id: str | None = None, limit: int = 50,
                      order: str = "desc") -> list[dict]:
        self._maybe_fail("list_sessions")
        if parent_id is None:
            return list(self.sessions.values())[:limit]
        return list(self.children.get(parent_id, []))[:limit]

    def messages(self, session_id: str, *, limit: int = 60, order: str = "desc",
                 type_: str | None = None) -> list[dict]:
        """V2 消息列表：按 (created, id) 排序；order=asc 最旧在前 / desc 最新在前。"""
        self._maybe_fail("messages")
        out = [m for m in self._messages_store.get(session_id, [])
               if not type_ or m.get("type") == type_]

        def key(m: dict) -> tuple[int, str]:
            t = m.get("time") or {}
            return (int(t.get("created") or 0), str(m.get("id") or ""))

        out.sort(key=key)
        if order == "desc":
            out.reverse()
        return out[:limit]

    def get_message(self, session_id: str, message_id: str) -> dict | None:
        """V2 定点消息契约：404 等价 None；命中按原样返回。"""
        self._maybe_fail("get_message")
        for m in self._messages_store.get(session_id, []):
            if m.get("id") == message_id:
                return m
        return None

    def message_page(self, session_id: str, *, limit: int = 50, order: str = "asc",
                     cursor: str | None = None) -> dict:
        """V2 消息分页契约：返回 {"data": [...], "cursor": {"previous","next"}}。

        - cursor 是不透明 token（"{direction}:{created}:{id}"）；首页按 order
          （asc 最旧在前 / desc 最新在前），沿 cursor 翻页按 token 中的方向
          继续，与真实服务“cursor 与 order 不组合”的契约一致。
        - 坏 cursor / 坏 anchor 按真实服务语义抛 400 ApiError(InvalidCursor)，
          不静默返回空页（协议错误必须可发现）。
        - message_scan_cap 限制可翻到的最早历史：更早页不存在，next=None，
          模拟“查询受限找不到输入锚点”的覆盖不足。
        - 空页（data=[]）与无后续页（next=None）是有效语义：由调用方结合
          游标判断终止；重复 cursor 的有界发现同样在消费者侧完成。
        """
        self._maybe_fail("message_page")
        msgs = self._visible_messages(session_id)  # 已按 (created, id) 升序

        def key(m: dict) -> tuple[int, str]:
            t = m.get("time") or {}
            return (int(t.get("created") or 0), str(m.get("id") or ""))

        path = f"/api/session/{session_id}/message"
        if cursor:
            parts = cursor.split(":", 2)
            bad_cursor = len(parts) != 3 or parts[0] not in ("asc", "desc")
            if not bad_cursor:
                try:
                    anchor = (int(parts[1]), parts[2])
                except ValueError:
                    bad_cursor = True
            if bad_cursor:
                raise ApiError(400, path, "InvalidCursor",
                               f"invalid cursor: {cursor[:60]}")
            direction = parts[0]
            remaining = [m for m in msgs if (
                key(m) > anchor if direction == "asc" else key(m) < anchor)]
        else:
            direction = order if order in ("asc", "desc") else "asc"
            remaining = list(msgs)
        if direction == "desc":
            remaining.reverse()
        data = remaining[:limit]
        result: dict = {"data": list(data), "cursor": {}}
        if data and len(remaining) > len(data):
            last = data[-1]
            result["cursor"]["next"] = f"{direction}:{key(last)[0]}:{key(last)[1]}"
        else:
            result["cursor"]["next"] = None
        if data:
            first_key = key(data[0])
            if direction == "asc" and msgs and first_key > key(msgs[0]):
                result["cursor"]["previous"] = (
                    f"desc:{first_key[0]}:{first_key[1]}")
            elif direction == "desc" and msgs and first_key < key(msgs[-1]):
                result["cursor"]["previous"] = (
                    f"asc:{first_key[0]}:{first_key[1]}")
            else:
                result["cursor"]["previous"] = None
        else:
            result["cursor"]["previous"] = None
        return result

    def inbox(self, session_id: str) -> list[dict]:
        self._maybe_fail("inbox")
        return list(self.inboxes.get(session_id, []))

    def permissions(self, directory: str) -> list[dict]:
        self._maybe_fail("permissions")
        # 真实适配器用查询参数 location[directory] 过滤；fake 用同名字段匹配。
        return [p for p in self.permission_requests if p.get("directory") == directory]

    def forms(self, session_id: str) -> list[dict]:
        self._maybe_fail("forms")
        return list(self._forms_store.get(session_id, []))

    def shells(self, directory: str) -> list[dict]:
        self._maybe_fail("shells")
        return list(self._shells_store.get(directory, []))

    def create_session(
        self,
        *,
        session_id: str,
        directory: str,
        title: str,
        agent: str | None = None,
        model: dict | None = None,
        metadata: dict | None = None,
        permissions: list[dict] | None = None,
    ) -> dict:
        self._maybe_fail("create_session")
        session = {
            "id": session_id, "title": title, "agent": agent, "model": model or {},
            "location": {"directory": directory}, "metadata": metadata or {},
            "outcome": None, "time": {},
        }
        self.sessions[session_id] = session
        self.create_calls.append(dict(session))
        return dict(session)

    def prompt(
        self,
        session_id: str,
        *,
        message_id: str,
        text: str,
        delivery: str = "queue",
        metadata: dict | None = None,
        resume: bool | None = None,
    ) -> dict:
        self.prompt_calls.append({
            "session_id": session_id, "message_id": message_id, "text": text,
            "delivery": delivery, "metadata": metadata, "resume": resume,
        })
        # 409 冲突但仍已受理：先把消息写回消息存储，再抛 409（供 reconcile 重查确认）
        if self.prompt_conflict_admits:
            msg = {"id": message_id, "type": "user", "text": text[:80],
                   "time": {"created": 0}}
            self._messages_store.setdefault(session_id, []).append(msg)
        self._maybe_fail("prompt")
        # V2 queue 语义：排队进 inbox（含 payload/delivery），不进消息列表。
        # inbox 排队只证明已受理，不证明已执行；消息要经服务端投递
        # （deliver_inbox）后才可见——与真实服务一致。
        self.inboxes.setdefault(session_id, []).append({
            "id": message_id, "sessionID": session_id, "type": "user",
            "time": {"created": 0},
            "payload": {"text": text[:80], "metadata": metadata or {}},
            "delivery": delivery,
        })
        self.inbox_lifecycle_events.append({
            "type": "session.inbox.enqueued",
            "data": {"sessionID": session_id, "inboxID": message_id},
        })
        if resume is not False:
            self.scheduled_sessions.add(session_id)
        return {"data": {"id": message_id}}

    def session_model(self, session_id: str) -> dict | None:
        session = self.sessions.get(session_id)
        if not isinstance(session, dict):
            return None
        model = session.get("model")
        return model if isinstance(model, dict) else None

    def switch_model(self, session_id: str, *, provider: str, model: str,
                     variant: str | None = None) -> dict:
        self.switch_calls.append({
            "session_id": session_id, "providerID": provider,
            "id": model, "variant": variant,
        })
        self._maybe_fail("switch_model")
        if session_id not in self.sessions:
            raise ApiError(404, "/api/session/" + session_id + "/model",
                           "SessionNotFoundError", "session not found", "")
        ref = {"providerID": provider, "id": model}
        if variant:
            ref["variant"] = variant
        self.sessions[session_id]["model"] = ref
        return {"status": 204, "accepted": True}

    def interrupt(self, session_id: str, resume: bool = False) -> dict:
        self.interrupt_calls.append({"session_id": session_id, "resume": resume})
        return {"data": {}}


def complete_execution(ledger, attempt_id: str, *, outcome: str = "succeeded",
                        ended_at: int | None = None,
                        input_at: int | None = None) -> dict:
    """测试夹具的合法证据入口：经 record_terminal 提交终态。

    区间证据取自尝试台账中真实记录的时间（started/dispatched/created），
    不伪造 input_at=1 之类的信任样本：input_at 默认取本轮输入的真实时刻，
    ended 默认在其后，保证 ended >= input_at 的顺序约束来自真实事实。

    需要更精确锚点（真实消息 created）的剧本应布置 FakeClient 消息并走
    supervisor REST 路径，而不是依赖本入口造证据。
    """
    att = ledger.get_attempt(attempt_id)
    input_anchor = input_at or att.get("started_at") or att.get("dispatched_at") \
        or att.get("created_at") or 0
    ended = ended_at or max(input_anchor,
                            att.get("started_at") or 0,
                            att.get("dispatched_at") or 0,
                            att.get("created_at") or 0, 1) + 1_000
    return ledger.record_terminal(
        attempt_id, outcome=outcome, source="rest",
        event_id=f"evt-fixture-{attempt_id}-{outcome}",
        candidate={
            "outcome": outcome, "source": "rest", "ended_at": ended,
            "session_id": att.get("session_id"),
            "interval": {"proven": True, "input_at": input_anchor,
                         "via": "fixture-ledger-facts"},
        })


__all__ = [
    "ApiError", "TransportError",  # 再导出让测试少一处 import
    "FakeClock", "FakeService", "FakeClient",
    "complete_execution",
]

"""OpenCode V2 服务发现、HTTP API 与 SSE 适配器实现。

本模块是传输实现；根包 `oc_orch.client` 是其兼容别名（sys.modules 指向同一模块），
保证既有 import 与 mock.patch 路径一致。

认证只在进程内存中使用：读取既有 service.json 的 host/port/password，
以 HTTP Basic 请求本机既有服务。不打印、不落盘、不复制凭据，也不把
该客户端用于任意远程主机。

消息端点（get_message/message_page）响应违反固封 V2 schema 时抛
MessageProtocolError（code=MESSAGE_PROTOCOL），不静默降级为空/
None——协议错误必须与“消息不存在/覆盖完整”可区分（收口复核 P2）。
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterator

from ..domain.errors import ApiError, OrchError, StreamError, TransportError
from ..util import json_dumps, service_config_path


class MessageProtocolError(OrchError):
    """V2 消息端点响应违反固封 schema（OpenCode 2.2.5 /openapi.json）。

    P2 修复（收口复核：分页协议降级错误）：message_page/get_message 收到
    非对象响应、缺 data/cursor 字段或字段类型错误时**不得**静默规整为
    空列表/null 游标/None——那会让调用方把协议错误当作“完整覆盖”或
    “消息不存在”。本错误携带窄原因码，供消费者（results.read_message_window、
    supervisor REST 补证）转换为 coverage 不足/查询失败，而不是猜测结果。

    - code 固定 ``MESSAGE_PROTOCOL``；`reason` 为窄原因码（见各方法 docstring）。
    - 与 ApiError（HTTP 层错误）和 TransportError（传输层错误）并列、可区分。
    """

    code = "MESSAGE_PROTOCOL"

    def __init__(self, reason: str, path: str, detail: dict | None = None):
        self.reason = reason
        self.path = path
        self.detail = detail or {}
        super().__init__(f"MESSAGE_PROTOCOL {reason} {path}")


@dataclass
class Service:
    base: str
    host: str
    port: int
    password: str
    scheme: str = "http"
    fingerprint: str = ""

    def public(self) -> dict:
        return {
            "base": self.base,
            "host": self.host,
            "port": self.port,
            "scheme": self.scheme,
            "config": service_config_path(),
        }

    def auth_header(self) -> str:
        cred = base64.b64encode(f"opencode:{self.password}".encode("utf-8")).decode("ascii")
        return f"Basic {cred}"


def load_service() -> Service:
    path = service_config_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        raise OrchError(f"找不到 OpenCode 服务配置：{path}") from None
    except (OSError, ValueError) as e:
        raise OrchError(f"读取 OpenCode 服务配置失败：{path}: {e}") from None

    url = (cfg.get("url") or "").strip()
    host = cfg.get("hostname") or "127.0.0.1"
    port = cfg.get("port")
    scheme = "http"
    if url:
        parsed = urllib.parse.urlsplit(url)
        scheme = parsed.scheme or "http"
        host = parsed.hostname or host
        port = parsed.port or port
    if not port:
        raise OrchError(f"服务配置缺少 port：{path}")
    base = url or f"{scheme}://{host}:{port}"
    password = cfg.get("password") or ""
    # 只用于比较配置是否变化；不输出。
    fp = json.dumps(
        {"host": host, "port": port, "password": password, "url": url},
        sort_keys=True,
    )
    return Service(base=base, host=host, port=int(port), password=password,
                   scheme=scheme, fingerprint=fp)


class Client:
    def __init__(self, timeout: float = 30.0, service: Service | None = None):
        self.timeout = timeout
        self.service = service or load_service()
        self._last_reload = time.time()

    # ---- 服务发现 -------------------------------------------------
    def reload_service(self) -> dict:
        old = self.service
        try:
            new = load_service()
        except OrchError:
            raise
        changed = (
            old.base != new.base
            or old.host != new.host
            or old.port != new.port
            or old.fingerprint != new.fingerprint
        )
        self.service = new
        self._last_reload = time.time()
        return {
            "changed": changed,
            "old_base": old.base,
            "base": new.base,
        }

    def maybe_reload(self) -> dict:
        if time.time() - self._last_reload < 2:
            return {"changed": False, "base": self.service.base}
        return self.reload_service()

    # ---- HTTP -----------------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        *,
        retry: bool = False,
        timeout: float | None = None,
    ) -> Any:
        # 即使内部调用方误传 retry=True，写请求也不能因掉响应而重放。
        retry = bool(retry and method.upper() == "GET")
        data = None
        headers = {
            "Authorization": self.service.auth_header(),
            "Accept": "application/json",
        }
        if body is not None:
            data = json_dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        url = self.service.base.rstrip("/") + path
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
                raw = resp.read()
                if not raw:
                    return None
                try:
                    return json.loads(raw)
                except ValueError:
                    return raw.decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            # 状态行已明确返回拒绝；错误体损坏不应吞掉 status 或变成未知传输错。
            try:
                error_body = e.read()
            except http.client.IncompleteRead as body_error:
                error_body = body_error.partial
            except (http.client.HTTPException, OSError):
                error_body = b""
            finally:
                e.close()
            raw = error_body.decode("utf-8", "replace")
            tag = None
            message = raw[:500]
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    tag = parsed.get("_tag") if isinstance(parsed.get("_tag"), str) else None
                    message = str(parsed.get("message") or message)
            except ValueError:
                pass
            if e.code == 401 and retry:
                try:
                    self.reload_service()
                except OrchError:
                    pass  # 保留已观察到的 401；配置问题不能掩盖认证分类。
            raise ApiError(e.code, path, tag, message, raw) from None
        except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError,
                OSError, http.client.HTTPException) as e:
            # GET 单次重试：先重新发现服务；重试结果（含 ApiError）权威，不吞掉分类。
            # reload 失败时不再请求同一坏端点，保留原始 TransportError。
            if retry:
                try:
                    self.reload_service()
                except OrchError:
                    pass
                else:
                    return self._request(method, path, body, retry=False, timeout=timeout)
            raise TransportError(path, repr(e)) from None

    def _get(self, path: str, params: dict | None = None) -> Any:
        if params:
            query = urllib.parse.urlencode(params, doseq=True)
            path = f"{path}?{query}"
        return self._request("GET", path, retry=True)

    def _post(self, path: str, body: dict | None = None) -> Any:
        return self._request("POST", path, body, retry=False)

    # ---- 只读 -----------------------------------------------------
    def info(self) -> dict:
        return self._get("/api/info")

    def get_message(self, session_id: str, message_id: str) -> dict | None:
        """定点消息读取（V2：GET /api/session/{sid}/message/{mid}）。

        - HTTP 404 → None：消息确实不存在（唯一合法的“不存在”语义）。
        - 其余响应必须符合固封形状（对象 + data 为对象）；空响应体、
          非对象响应、缺 data 字段、data 非/null 一律抛 MessageProtocolError
          （reason：empty_body/body_not_object/data_missing/data_wrong_type）。
          坏响应与真实不存在必须可区分：调用方不得把协议错误当不存在。
        - 其他 HTTP 错误（401/403/5xx 等）原样抛 ApiError，不吞成 None。
        """
        path = f"/api/session/{session_id}/message/{message_id}"
        try:
            out = self._get(path)
        except ApiError as e:
            if e.status == 404:
                return None
            raise
        if not isinstance(out, dict):
            reason = "empty_body" if out is None else "body_not_object"
            raise MessageProtocolError(reason, path, {"got": type(out).__name__})
        if "data" not in out:
            raise MessageProtocolError("data_missing", path, {"keys": sorted(out)[:8]})
        data = out["data"]
        if not isinstance(data, dict):
            raise MessageProtocolError("data_wrong_type", path,
                                       {"got": type(data).__name__})
        return data

    def message_page(self, session_id: str, *, limit: int = 50, order: str = "asc",
                     cursor: str | None = None) -> dict:
        """分页消息读取（V2：GET /api/session/{sid}/message）。

        固封契约（OpenCode 2.2.5 SessionMessagesResponse）：

        - ``cursor`` 有值（非空字符串）时**绝不**传 order（服务契约禁止
          cursor/order 组合）；无 cursor 时传 order 指定首页方向。
        - 返回 ``{"data": [...], "cursor": {"previous": ..., "next": ...}}``；
          data 必须是数组、每项必须是对象，cursor 必须是对象。
        - 协议错误一律抛 MessageProtocolError，不静默降级（P2 修复）：
          reason ∈ {empty_body, body_not_object, data_missing, data_wrong_type,
          data_item_not_object, cursor_missing, cursor_wrong_type,
          cursor_field_wrong_type}。缺 cursor 曾经被规整为空游标并报告
          “完整覆盖”，已废弃。
        - ``cursor.previous/next`` 在固封 schema 中非必填：缺失等价 null；
          出现非字符串/非 null 值视为协议错误。
        - 空页（data=[]）与 next=null 的终页都是**有效**语义，正常返回，
          由调用方结合游标判断终止与重复游标。
        """
        path = f"/api/session/{session_id}/message"
        effective_cursor = cursor if isinstance(cursor, str) and cursor else None
        params: dict[str, Any] = {"limit": str(limit)}
        if effective_cursor is not None:
            params["cursor"] = effective_cursor
        else:
            params["order"] = order
        out = self._get(path, params)
        if not isinstance(out, dict):
            reason = "empty_body" if out is None else "body_not_object"
            raise MessageProtocolError(reason, path, {"got": type(out).__name__})
        if "data" not in out:
            raise MessageProtocolError("data_missing", path, {"keys": sorted(out)[:8]})
        data = out["data"]
        if not isinstance(data, list):
            raise MessageProtocolError("data_wrong_type", path,
                                       {"got": type(data).__name__})
        if "cursor" not in out:
            raise MessageProtocolError("cursor_missing", path,
                                       {"keys": sorted(out)[:8]})
        raw_cursor = out["cursor"]
        if not isinstance(raw_cursor, dict):
            raise MessageProtocolError("cursor_wrong_type", path,
                                       {"got": type(raw_cursor).__name__})
        prev = raw_cursor.get("previous")  # schema 非必填：缺失等价 null
        nxt = raw_cursor.get("next")
        for field, value in (("previous", prev), ("next", nxt)):
            if value is not None and not isinstance(value, str):
                raise MessageProtocolError("cursor_field_wrong_type", path,
                                           {"field": field,
                                            "got": type(value).__name__})
        for item in data:
            if not isinstance(item, dict):
                raise MessageProtocolError("data_item_not_object", path,
                                           {"got": type(item).__name__})
        return {"data": data, "cursor": {"previous": prev, "next": nxt}}

    def list_sessions(self, *, parent_id: str | None = None, limit: int = 50,
                      order: str = "desc") -> list[dict]:
        params: dict[str, Any] = {"limit": str(limit), "order": order}
        if parent_id:
            params["parentID"] = parent_id
        out = self._get("/api/session", params) or {}
        return out.get("data", [])

    def get_session(self, session_id: str) -> dict | None:
        try:
            out = self._get(f"/api/session/{session_id}")
        except ApiError as e:
            if e.status == 404:
                return None
            raise
        return (out or {}).get("data")

    def session_model(self, session_id: str) -> dict | None:
        """读取 session 当前模型（V2：GET /api/session/{sid} → data.model）。

        返回 ``{"providerID", "id", "variant?"}`` 或 None（404/缺字段）。
        只用于与切换请求对账，不能证明模型已产生有效工作。
        """
        session = self.get_session(session_id)
        if not isinstance(session, dict):
            return None
        model = session.get("model")
        if not isinstance(model, dict):
            return None
        return model

    def switch_model(self, session_id: str, *, provider: str, model: str,
                     variant: str | None = None) -> dict:
        """同会话切换后续回合模型（V2：POST /api/session/{sid}/model）。

        - 请求体 ``{"model": {"providerID", "id", "variant?"}}``（OpenAPI
          operationId ``session.switchModel``）；204 无正文视为受理。
        - 写请求**绝不自动重试**：HTTP 错误（400/401/404/5xx）原样抛
          ApiError，传输层错误抛 TransportError——调用方按“结果未知”对账，
          不得因返回异常而猜测失败或重放。
        - 返回的 ``accepted=True`` 只表示服务端受理切换请求，不表示切换
          已生效；调用方随后以 ``session_model`` 确认。
        """
        body: dict[str, Any] = {
            "model": {"providerID": provider, "id": model},
        }
        if variant:
            body["model"]["variant"] = variant
        self._post(f"/api/session/{session_id}/model", body)
        return {"status": 204, "accepted": True}

    def active_sessions(self) -> dict[str, dict]:
        path = "/api/session/active"
        out = self._get(path)
        if not isinstance(out, dict) or not isinstance(out.get("data"), dict):
            raise MessageProtocolError("active-data-not-object", path)
        if any(not isinstance(value, dict) for value in out["data"].values()):
            raise MessageProtocolError("active-entry-not-object", path)
        return out["data"]

    def messages(self, session_id: str, *, limit: int = 60, order: str = "desc",
                 type_: str | None = None) -> list[dict]:
        params: dict[str, Any] = {"limit": str(limit), "order": order}
        if type_:
            params["type"] = type_
        out = self._get(f"/api/session/{session_id}/message", params) or {}
        return out.get("data", [])

    def inbox(self, session_id: str) -> list[dict]:
        path = f"/api/session/{session_id}/inbox"
        out = self._get(path)
        if not isinstance(out, dict) or not isinstance(out.get("data"), list):
            raise MessageProtocolError("inbox-data-not-array", path)
        if any(not isinstance(item, dict) or not isinstance(item.get("id"), str)
               or not item["id"] for item in out["data"]):
            raise MessageProtocolError("inbox-entry-invalid", path)
        return out["data"]

    def update_inbox_delivery(self, session_id: str, inbox_id: str, *,
                              delivery: str = "steer") -> None:
        """变更原输入调度；写请求不重放，HTTP 成功不等于已执行。"""
        if delivery not in ("queue", "steer"):
            raise ValueError("delivery 必须为 queue 或 steer")
        self._request("PATCH", f"/api/session/{session_id}/inbox/{inbox_id}",
                      {"delivery": delivery}, retry=False)

    def cancel_inbox(self, session_id: str, inbox_id: str) -> None:
        """取消原 pending 项；服务端允许 no-op，调用方仍需对账结果。"""
        self._request("DELETE", f"/api/session/{session_id}/inbox/{inbox_id}",
                      retry=False)

    def permissions(self, directory: str) -> list[dict]:
        out = self._get(
            "/api/permission/request",
            {"location[directory]": directory},
        ) or {}
        return out.get("data", [])

    def forms(self, session_id: str) -> list[dict]:
        out = self._get(f"/api/session/{session_id}/form") or {}
        return out.get("data", [])

    def shells(self, directory: str) -> list[dict]:
        out = self._get("/api/shell", {"location[directory]": directory}) or {}
        return out.get("data", [])

    def session_diff(self, session_id: str) -> list[dict]:
        out = self._get(f"/api/session/{session_id}/diff") or {}
        return out.get("data", [])

    # ---- 写入 -----------------------------------------------------
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
        body: dict[str, Any] = {
            "id": session_id,
            "title": title,
            "location": {"directory": directory},
            "metadata": metadata or {},
        }
        if agent:
            body["agent"] = agent
        if model:
            body["model"] = model
        if permissions is not None:
            body["permissions"] = permissions
        out = self._post("/api/session", body) or {}
        return out.get("data", {})

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
        body: dict[str, Any] = {
            "id": message_id,
            "text": text,
            "delivery": delivery,
        }
        if metadata:
            body["metadata"] = metadata
        if resume is not None:
            body["resume"] = resume
        out = self._post(f"/api/session/{session_id}/prompt", body) or {}
        return out.get("data", {})

    def interrupt(self, session_id: str, resume: bool = False) -> dict:
        path = f"/api/session/{session_id}/interrupt?resume={'true' if resume else 'false'}"
        return self._post(path) or {}

    def reply_permission(self, session_id: str, request_id: str,
                         decision: str, message: str | None = None) -> None:
        body: dict[str, Any] = {"decision": decision}
        if message:
            body["message"] = message
        self._post(f"/api/session/{session_id}/permission/{request_id}/reply", body)

    def delete_session(self, session_id: str) -> None:
        self._request("DELETE", f"/api/session/{session_id}", retry=False)

    # ---- SSE ------------------------------------------------------
    def event_path(self, types: list[str]) -> str:
        query = urllib.parse.urlencode([("types", t) for t in types])
        return f"/api/event?{query}"

    def iter_events(self, types: list[str], *, socket_timeout: float = 90.0
                    ) -> Iterator[dict]:
        """连接一次 SSE；调用方负责重连与对账。

        yield 的第一项是 {"type": "server.connected", "data": {}}；
        随后是原始事件 dict。EOF/超时/HTTP 错误抛 StreamError。
        """
        svc = self.service
        conn_cls = http.client.HTTPSConnection if svc.scheme == "https" else http.client.HTTPConnection
        conn = conn_cls(svc.host, svc.port, timeout=socket_timeout)
        path = self.event_path(types)
        try:
            conn.request(
                "GET",
                path,
                headers={
                    "Authorization": svc.auth_header(),
                    "Accept": "text/event-stream",
                    "Cache-Control": "no-cache",
                },
            )
            resp = conn.getresponse()
            if resp.status != 200:
                body = resp.read(500).decode("utf-8", "replace")
                raise StreamError(f"HTTP {resp.status}: {body[:300]}")
            try:
                conn.sock.settimeout(socket_timeout)
            except (AttributeError, OSError):
                pass
            frame: list[str] = []
            while True:
                raw = resp.readline()
                if not raw:
                    raise StreamError("EOF")
                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if line != "":
                    frame.append(line)
                    continue
                data_lines = [ln[5:].strip() for ln in frame if ln.startswith("data:")]
                frame = []
                if not data_lines:
                    continue
                payload = "\n".join(data_lines)
                try:
                    event = json.loads(payload)
                except ValueError:
                    continue
                if isinstance(event, str):
                    try:
                        event = json.loads(event)
                    except ValueError:
                        continue
                if isinstance(event, dict):
                    yield event
        except (socket.timeout, TimeoutError) as e:
            raise StreamError(f"read-timeout: {e!r}") from None
        except (http.client.HTTPException, OSError) as e:
            raise StreamError(f"connection: {e!r}") from None
        finally:
            try:
                conn.close()
            except Exception:
                pass

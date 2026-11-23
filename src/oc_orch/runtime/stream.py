"""SSE 监听线程：只做连接、解析、入队；所有业务写入由主线程完成。

官方 V2 事件流 live-only，不自动重放。因此本模块不把“收到事件”当作完整事实：
主线程每条连接建立后、以及每次重连后都执行一次 REST 对账。
"""

from __future__ import annotations

import queue
import threading

from ..application.ports import OpenCodeClient
from ..domain.classify import SUBSCRIBE_TYPES
from ..domain.errors import StreamError
from ..util import now_ms


class EventStream(threading.Thread):
    def __init__(self, client: OpenCodeClient, cfg: dict, out: "queue.Queue[dict]"):
        super().__init__(name="oc-orch-sse", daemon=True)
        self.client = client
        self.cfg = cfg
        self.out = out
        self.stop_event = threading.Event()
        self.stats = {
            "connects": 0,
            "disconnects": 0,
            "events": 0,
            "content_ignored": 0,
            "last_connected_at": None,
            "last_event_at": None,
            "last_error": None,
            "backoff": 0,
        }

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        backoff = 1
        while not self.stop_event.is_set():
            try:
                # 每次连接前重新读取 service.json：端点/密码变化后自动跟随
                try:
                    self.client.reload_service()
                except Exception as e:
                    self.out.put({"kind": "error", "error": repr(e), "at": now_ms()})
                self.stats["connects"] += 1
                connected = False
                for envelope in self.client.iter_events(
                    SUBSCRIBE_TYPES,
                    socket_timeout=float(self.cfg["event_socket_timeout"]),
                ):
                    if self.stop_event.is_set():
                        return
                    etype = envelope.get("type")
                    if etype == "server.connected":
                        connected = True
                        backoff = 1
                        self.stats["last_connected_at"] = now_ms()
                        self.stats["backoff"] = 0
                        self.out.put({"kind": "connected", "at": now_ms()})
                        continue
                    self.stats["events"] += 1
                    self.stats["last_event_at"] = now_ms()
                    self.out.put({"kind": "event", "event": envelope, "at": now_ms()})
            except StreamError as e:
                self.stats["disconnects"] += 1
                self.stats["last_error"] = str(e)
                self.out.put({"kind": "disconnected", "error": str(e), "at": now_ms()})
            except Exception as e:  # 防御：线程不能因未预期异常退出
                self.stats["disconnects"] += 1
                self.stats["last_error"] = repr(e)
                self.out.put({"kind": "disconnected", "error": repr(e), "at": now_ms()})
            if self.stop_event.is_set():
                return
            self.stats["backoff"] = backoff
            # 指数退避；有界，避免服务不可用时忙转
            self.stop_event.wait(backoff)
            backoff = min(backoff * 2, 30)

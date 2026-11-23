"""应用层端口契约：外部依赖（OpenCode 服务、时间）的窄接口。

设计要点（见 OpenSpec modularize-orchestrator design Decision 4）：
- Clock.now_ms() 用于持久化时间戳/期限（重启后可解释的 wall time）；
  Clock.monotonic() 只用于单进程内的等待间隔；二者不得混用。
- Clock.sleep 允许注入为“不真实等待”，使故障剧本由受控时间驱动。
- OpenCodeClient 只声明 runtime/应用服务实际使用的方法；默认实现是
  oc_orch.adapters.opencode.Client（经 oc_orch.client 门面导入），
  测试使用 tests/fakes.FakeClient。
- 本模块只声明协议；具体时钟默认装配 SystemClock 位于 adapters/clock.py。
"""

from __future__ import annotations

from typing import Any, Iterator, Protocol


class Clock(Protocol):
    """可注入时钟。wall 用于持久 deadline，monotonic 用于单进程等待间隔。"""

    def now_ms(self) -> int:
        """当前 wall time（毫秒）；持久化时间戳/期限必须用本方法。"""
        ...

    def monotonic(self) -> int:
        """单调时间（毫秒）；只用于单进程内间隔/超时等待，不得持久化。"""
        ...

    def sleep(self, seconds: float) -> None:
        """睡眠指定秒数；测试时钟可注入为不真实等待。"""
        ...


class OpenCodeClient(Protocol):
    """OpenCode V2 窄端口：Supervisor/应用服务所依赖的查询与写入。"""

    service: Any

    def reload_service(self) -> dict: ...

    def iter_events(self, types: list[str], *, socket_timeout: float = 90.0
                    ) -> Iterator[dict]: ...

    def info(self) -> dict: ...

    def active_sessions(self) -> dict[str, dict]: ...

    def get_session(self, session_id: str) -> dict | None: ...

    def get_message(self, session_id: str, message_id: str) -> dict | None: ...

    def session_model(self, session_id: str) -> dict | None: ...

    def switch_model(self, session_id: str, *, provider: str, model: str,
                     variant: str | None = None) -> dict: ...

    def message_page(self, session_id: str, *, limit: int = 50, order: str = "asc",
                     cursor: str | None = None) -> dict: ...

    def list_sessions(self, *, parent_id: str | None = None, limit: int = 50,
                      order: str = "desc") -> list[dict]: ...

    def messages(self, session_id: str, *, limit: int = 60, order: str = "desc",
                 type_: str | None = None) -> list[dict]: ...

    def inbox(self, session_id: str) -> list[dict]: ...

    def update_inbox_delivery(self, session_id: str, inbox_id: str, *,
                              delivery: str = "steer") -> None: ...

    def cancel_inbox(self, session_id: str, inbox_id: str) -> None: ...

    def permissions(self, directory: str) -> list[dict]: ...

    def forms(self, session_id: str) -> list[dict]: ...

    def shells(self, directory: str) -> list[dict]: ...

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
    ) -> dict: ...

    def prompt(
        self,
        session_id: str,
        *,
        message_id: str,
        text: str,
        delivery: str = "queue",
        metadata: dict | None = None,
        resume: bool | None = None,
    ) -> dict: ...

    def interrupt(self, session_id: str, resume: bool = False) -> dict: ...

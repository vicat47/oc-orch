"""独立资源授权共享契约：只读检查不授予动作许可；装配由组合根完成。

本模块只依赖窄 Protocol 与 domain 稳定错误码，不导入 adapter；authorize
只反映“该模型在当前独立授权版本内被允许”，不是单次动作许可。
"""

from __future__ import annotations

from typing import Protocol


class ResourceAuthorizationPort(Protocol):
    def replace(self, scope: str, scope_id: str, *, resources: list, expires_at: int,
                expected_revision: int, operation_id: str, actor: str,
                reason: str) -> dict: ...

    def snapshot(self, scope: str, scope_id: str) -> dict: ...

    def authorize(self, scope: str, scope_id: str, *, model: str, preset: str,
                  now: int | None = None) -> dict: ...


class ResourceAuthorizationService:
    """无Client、无网络；authorize只检查当前独立授权记录，不产生单次许可。"""

    def __init__(self, store: ResourceAuthorizationPort):
        self._store = store

    def replace(self, scope: str, scope_id: str, **intent) -> dict:
        return self._store.replace(scope, scope_id, **intent)

    def snapshot(self, scope: str, scope_id: str) -> dict:
        return self._store.snapshot(scope, scope_id)

    def authorize(self, scope: str, scope_id: str, *, model: str, preset: str,
                  now: int | None = None) -> dict:
        return self._store.authorize(scope, scope_id, model=model, preset=preset, now=now)
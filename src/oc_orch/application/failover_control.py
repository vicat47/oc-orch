"""Failover operator use cases; persistent storage and installation come from composition."""

from __future__ import annotations

from typing import Callable, Protocol


class FailoverAdminPort(Protocol):
    def guard_valid(self) -> dict: ...
    def get_policy(self, scope: str, scope_id: str) -> dict | None: ...
    def set_policy(self, scope: str, scope_id: str, policy: dict, **options) -> dict: ...
    def release_session_lock(self, session_id: str, **options) -> dict: ...
    def clear_route_blocked(self, route: str, **options) -> dict: ...


class FailoverControl:
    def __init__(self, store: FailoverAdminPort, *,
                 install_guard: Callable[[str], dict],
                 task_project: Callable[[str], str | None]):
        self.store = store
        self._install_guard = install_guard
        self._task_project = task_project

    def contract_status(self) -> dict:
        return self.store.guard_valid()

    def install_contract(self, *, actor: str = "operator") -> dict:
        return self._install_guard(actor)

    def show_policy(self, scope: str, scope_id: str, *,
                    project_default: str | None = None) -> dict:
        current = self.store.get_policy(scope, scope_id)
        project = project_default
        if scope == "task" and not project:
            project = self._task_project(scope_id)
        inherited = (self.store.get_policy("project", project)
                     if scope == "task" and project else current)
        return {"ok": True, "scope": scope, "scope_id": scope_id,
                "project": project, "guard": self.store.guard_valid(),
                "current": current, "project_default": inherited,
                "effective": current or inherited}

    def set_policy(self, scope: str, scope_id: str, policy: dict, *,
                   expected_revision: int | None, operation_id: str,
                   reason: str, actor: str = "operator") -> dict:
        return self.store.set_policy(
            scope, scope_id, policy, expected_revision=expected_revision,
            operation_id=operation_id, reason=reason, actor=actor)

    def release_session(self, session_id: str, *, operation_id: str,
                        reason: str, actor: str = "operator") -> dict:
        return self.store.release_session_lock(
            session_id, operation_id=operation_id, reason=reason, actor=actor)

    def clear_route(self, route: str, *, reason: str) -> dict:
        return self.store.clear_route_blocked(route, reason=reason)

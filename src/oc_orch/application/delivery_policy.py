"""共享策略用例；版本/投影不直接授予单次资源动作许可。"""

from __future__ import annotations

from typing import Protocol

from ..domain.delivery_policy import render_instructions


class DeliveryPolicyPort(Protocol):
    def project_snapshot(self, project: str) -> dict: ...

    def task_snapshot(self, task_id: str) -> dict: ...

    def set_project_default(self, project: str, *, policy: dict,
                            expected_revision: int, operation_id: str,
                            actor: str, reason: str) -> dict: ...

    def switch_task(self, task_id: str, *, policy: dict | None,
                    expected_revision: int, operation_id: str,
                    actor: str, reason: str, inherit: bool = False) -> dict: ...


class DeliveryPolicyService:
    """无Client、无网络；版本保存成功不等于运行时策略已经执行。"""

    def __init__(self, store: DeliveryPolicyPort, *, views=None):
        self._store = store
        self._views = views

    def project(self, project: str) -> dict:
        if self._views is not None:
            return self._views.read("project", project)
        return self._store.project_snapshot(project)

    def task(self, task_id: str) -> dict:
        if self._views is not None:
            return self._views.read("task", task_id)
        view = self._store.task_snapshot(task_id)
        return {**view, "instructions": (render_instructions(view["version"])
                                         if view["managed"] else None)}

    def set_project_default(self, project: str, **intent) -> dict:
        result = self._store.set_project_default(project, **intent)
        current = self.project(project)
        return {**result, "current": current, "execution_enforced": current["execution_enforced"]}

    def switch_task(self, task_id: str, **intent) -> dict:
        result = self._store.switch_task(task_id, **intent)
        current = self.task(task_id)
        return {**result, "current": current, "execution_enforced": current["execution_enforced"]}

"""插件与本地控制面的精确控制快照；不依赖任务分页，不发现远端服务。"""

from .coordinator_control import CoordinatorControlPort
from ..domain.errors import LedgerError


class ProjectControlView:
    def __init__(self, control: CoordinatorControlPort, *, read_snapshot, identity):
        self._control, self._read_snapshot, self._identity = control, read_snapshot, identity

    def show(self, *, project, session_id, operation_id=None):
        for key, value in (("project", project), ("session_id", session_id),
                           ("operation_id", operation_id)):
            if key == "operation_id" and value is None:
                continue
            if not isinstance(value, str) or not value or value != value.strip():
                raise LedgerError("INVALID_SCOPE", f"{key}须为原样非空身份")
        with self._read_snapshot():
            project_snap = self._control.get_project(project)
            session_snap = self._control.get_session(session_id)
            lease = self._control.coordinator_lease(project)
            operation = (self._control.project_operation(operation_id, project, session_id)
                         if operation_id is not None else None)
            return {
                "ok": True, "schema": "oc-orch.project-control.v1",
                "project": project, "session_id": session_id,
                "project_control": {k: project_snap[k] for k in ("status", "revision")},
                "session_control": {
                    "held": session_snap["hold"] is not None,
                    "revision": session_snap["revision"],
                    "user_turn_active": session_snap["barrier"] is not None,
                    "hold": session_snap["hold"],
                    "barrier": session_snap["barrier"],
                    "watermark": session_snap["watermark"],
                },
                "lease": ({k: lease.get(k) for k in (
                    "holder_session", "epoch", "expires_at", "recovery_blocked")}
                    if lease is not None else None),
                "operation": operation, "runtime": self._identity(),
            }

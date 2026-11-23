"""策略、独立授权、在途和运行身份共用一个只读WAL快照；不构造Client。"""

from ..domain.delivery_policy import render_instructions
from ..domain.route_context import route_context
from .delivery_action import DeliveryActions
from .delivery_policy import DeliveryPolicyStore
from .release_identity import runtime_identity
from .sqlite_db import read_snapshot


class DeliveryPolicyViews:
    def __init__(self, ledger, clock):
        self.ledger, self.conn, self.clock = ledger, ledger.conn, clock

    def read(self, scope, scope_id):
        from .resource_authorization import ResourceAuthorizationStore

        with read_snapshot(self.conn):
            policies = DeliveryPolicyStore(self.conn, self.clock)
            view = policies.task_snapshot(scope_id) if scope == "task" else policies.project_snapshot(scope_id)
            authorizations = ResourceAuthorizationStore(self.conn, self.clock)
            auth = authorizations.snapshot(scope, scope_id)
            if scope == "task" and not auth["revision"]:
                auth = authorizations.snapshot("project", view["project"])
            runtime = runtime_identity(self.conn)
            out = {**view, "storage_schema": view["schema"], "schema": "oc-orch.delivery-policy.v1",
                   "runtime": runtime, "authorization": auth,
                   "instructions": render_instructions(view["version"]) if view["managed"] else None,
                   "instruction_target": None, "inflight": {"items": [], "total": 0, "truncated": False},
                   "scope_effect": "new-tasks-only" if scope == "project" else "subsequent-safe-decisions-only",
                   "legacy_compatibility": not view["managed"],
                   "limits": ["不拦截OpenCode内部每次调用/重试/委派；不保证全链免费或严格金额上限",
                              "共用协调者费用授权独立，Task策略不改绑父模型",
                              "公开prompt/inbox不原子绑定模型；实际宿主/路线隔离仍须单独核验"]}
            if scope == "task":
                out["inflight"] = DeliveryActions(self.ledger, self.clock).task_inflight(scope_id)
                task = self.ledger.get_task(scope_id)
                sid = task.get("coordinator_session")
                out["coordinator_authorization"] = authorizations.snapshot("coordinator", sid) if sid else None
                att = self.ledger.get_attempt(task["current_attempt_id"]) if task.get("current_attempt_id") else None
                out["routing_context"] = route_context(task, att, self.clock.now_ms())
                if view["managed"] and view["execution_enforced"] and att and att.get("session_id"):
                    session_id = att["session_id"]
                    other_tasks = self.conn.execute(
                        "SELECT COUNT(DISTINCT task_id) FROM attempts WHERE session_id=?", (session_id,)).fetchone()[0]
                    coordinator = self.conn.execute(
                        "SELECT 1 FROM tasks WHERE coordinator_session=? UNION ALL "
                        "SELECT 1 FROM coordinator_leases WHERE holder_session=? LIMIT 1",
                        (session_id, session_id)).fetchone()
                    if (other_tasks == 1 and not coordinator and att.get("site_released_at") is None
                            and att.get("stop_requested_at") is None):
                        out["instruction_target"] = {"session_id": session_id, "task_id": scope_id,
                                                     "next_safe_step_only": True}
            return out

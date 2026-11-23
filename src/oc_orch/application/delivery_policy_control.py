"""CLI/Web/插件共用显式意图校验；confirm/CAS不是身份认证或费用授权。"""

from ..domain.delivery_policy import identity, normalize_policy, revision
from ..domain.errors import LedgerError


class DeliveryPolicyControl:
    def __init__(self, service):
        self.service = service

    def show(self, *, task_id=None, project=None):
        if (task_id is None) == (project is None):
            raise LedgerError("DELIVERY_POLICY_INVALID", "必须且只能显式指定task_id或project")
        identity(task_id if task_id is not None else project, "scope_id")
        return self.service.task(task_id) if task_id is not None else self.service.project(project)

    def apply(self, scope: str, body: dict, *, actor: str):
        key = "task_id" if scope == "task" else "project"
        allowed = {key, "policy", "expected_revision", "operation_id", "reason", "confirm"}
        if scope == "task":
            allowed.add("inherit")
        if scope not in ("task", "project") or not isinstance(body, dict) or set(body) - allowed:
            raise LedgerError("DELIVERY_POLICY_INVALID", "策略入口作用域/字段不合法")
        scope_id = identity(body.get(key), key)
        if body.get("confirm") != scope_id:
            raise LedgerError("DELIVERY_POLICY_CONFIRM_REQUIRED", "须逐字回填目标身份，取消/空白不执行")
        revision(body.get("expected_revision"))
        identity(body.get("operation_id"), "operation_id")
        identity(body.get("reason"), "reason")
        intent = {k: body[k] for k in ("expected_revision", "operation_id", "reason")}
        intent["actor"] = actor  # 入口归属固定，不允许body自报改写审计主体
        if "inherit" in body and type(body["inherit"]) is not bool:
            raise LedgerError("DELIVERY_POLICY_INVALID", "inherit必须是bool")
        inherit = body.get("inherit", False)
        if inherit and "policy" in body:
            raise LedgerError("DELIVERY_POLICY_INVALID", "继承与policy不能同时提交")
        if not inherit:
            intent["policy"] = normalize_policy(body.get("policy"))
        if scope == "task":
            return self.service.switch_task(scope_id, **intent, inherit=inherit)
        return self.service.set_project_default(scope_id, **intent)

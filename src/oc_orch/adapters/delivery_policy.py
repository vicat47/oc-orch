"""meta/audit策略版本、CAS和幂等；保留S1的draft键兼容，不自动启用。

只有独立delivery写契约显式安装且本作用域有版本，才标记本地输入许可接线。
旧Task不因全局写门安装自动纳管；该标志不代表内部费用/真实宿主已验收。
"""

from __future__ import annotations

import json
import sqlite3

from ..domain import delivery_policy as rules
from ..domain.errors import LedgerError
from .sqlite_db import read_snapshot, transaction

PREFIX = "delivery-policy-draft:v1:"


def _corrupt(message):
    raise LedgerError("DELIVERY_POLICY_CORRUPT", message)


def _key(kind: str, *identity) -> str:
    return PREFIX + kind + ":" + rules.canonical(identity)


class DeliveryPolicyStore:
    def __init__(self, conn: sqlite3.Connection, clock):
        self.conn, self.clock = conn, clock

    def _load(self, key):
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row is None:
            return None
        try:
            value = json.loads(row[0])
        except (TypeError, ValueError) as exc:
            raise LedgerError("DELIVERY_POLICY_CORRUPT", "策略JSON损坏") from exc
        if not isinstance(value, dict):
            _corrupt("策略记录不是对象")
        return value

    def _insert(self, key, value):
        self.conn.execute("INSERT INTO meta(key,value) VALUES(?,?)",
                          (key, rules.canonical(value)))

    def _task_project(self, task_id):
        rules.identity(task_id, "task_id")
        row = self.conn.execute("SELECT project FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise LedgerError("DELIVERY_POLICY_TASK_MISSING", "任务不存在")
        return rules.identity(row[0], "project")

    def _version(self, scope, scope_id, project, revision):
        record = self._load(_key("version", scope, scope_id, revision))
        if record is None:
            _corrupt("策略head/operation引用的版本缺失")
        try:
            rules.validate_version(record, scope=scope, scope_id=scope_id,
                                   project=project, expected_revision=revision)
        except LedgerError as exc:
            raise LedgerError("DELIVERY_POLICY_CORRUPT", "持久策略版本非法") from exc
        # 版本、head、操作索引必须共同存在；半损坏不能重建索引后冒认新操作。
        operation = self._load(_key("operation", record["operation_id"]))
        expected_operation = {
            "schema": rules.SCHEMA, "operation_id": record["operation_id"],
            "scope": scope, "scope_id": scope_id, "project": project,
            "revision": revision, "fingerprint": rules.fingerprint({
                "scope": scope, "scope_id": scope_id,
                "policy": None if record["source"] is not None else record["policy"],
                "inherit": record["source"] is not None,
                "expected_revision": revision - 1, "operation_id": record["operation_id"],
                "actor": record["actor"], "reason": record["reason"]})}
        if (operation != expected_operation
                or type(operation.get("schema")) is not int
                or type(operation.get("revision")) is not int):
            _corrupt("策略版本与操作索引缺失或不一致")
        if record["source"] is not None:
            source = record["source"]
            original = self._version("project", project, project, source["revision"])
            if original["policy"] != record["policy"]:
                _corrupt("继承值与来源版本不匹配")
        return record

    def _snapshot(self, scope, scope_id, project):
        from .delivery_guard import check_delivery_guard

        enforced = check_delivery_guard(self.conn)
        head = self._load(_key("head", scope, scope_id))
        version = None
        if head is not None:
            if (set(head) != {"schema", "scope", "scope_id", "project", "revision"}
                    or type(head["schema"]) is not int or head["schema"] != rules.SCHEMA
                    or head["scope"] != scope or head["scope_id"] != scope_id
                    or head["project"] != project or type(head["revision"]) is not int
                    or head["revision"] < 1):
                _corrupt("策略head身份/schema/revision非法")
            version = self._version(scope, scope_id, project, head["revision"])
        else:
            # 不能把被误删的head读成从未纳管。
            prefix = _key("version", scope, scope_id)[:-1] + ","
            orphan = self.conn.execute(
                "SELECT 1 FROM meta WHERE key>=? AND key<? LIMIT 1",
                (prefix, prefix + "\uffff")).fetchone()
            if orphan is not None:
                _corrupt("已有策略版本但head缺失")
        return {"ok": True, "schema": "oc-orch.delivery-policy-draft.v1",
                "scope": scope, "scope_id": scope_id, "project": project,
                "managed": version is not None,
                "revision": version["revision"] if version else 0,
                "version": version, "execution_enforced": enforced and version is not None,
                "coverage": {"persistence": True, "writer_guard": enforced,
                             "resource_authorization": enforced and version is not None,
                             "action_permits": enforced and version is not None,
                             "runtime": enforced and version is not None,
                             "internal_model_calls": False, "whole_chain_free_guarantee": False}}

    def project_snapshot(self, project: str) -> dict:
        rules.identity(project, "project")
        with read_snapshot(self.conn):
            return self._snapshot("project", project, project)

    def task_snapshot(self, task_id: str) -> dict:
        with read_snapshot(self.conn):
            return self._snapshot("task", task_id, self._task_project(task_id))

    def set_project_default(self, project: str, *, policy: dict,
                            expected_revision: int, operation_id: str,
                            actor: str, reason: str) -> dict:
        rules.identity(project, "project")
        return self._switch("project", project, policy=policy, inherit=False,
                            expected_revision=expected_revision, operation_id=operation_id,
                            actor=actor, reason=reason)

    def switch_task(self, task_id: str, *, policy: dict | None = None,
                    expected_revision: int, operation_id: str,
                    actor: str, reason: str, inherit: bool = False) -> dict:
        rules.identity(task_id, "task_id")
        return self._switch("task", task_id, policy=policy, inherit=inherit,
                            expected_revision=expected_revision, operation_id=operation_id,
                            actor=actor, reason=reason)

    def _switch(self, scope, scope_id, *, policy, inherit, expected_revision,
                operation_id, actor, reason):
        for label, value in (("operation_id", operation_id), ("actor", actor), ("reason", reason)):
            rules.identity(value, label)
        rules.revision(expected_revision)
        if type(inherit) is not bool or (inherit and scope != "task"):
            rules.invalid("inherit必须是任务级bool")
        if inherit and policy is not None:
            rules.invalid("inherit不得同时携带policy")
        normalized = None if inherit else rules.normalize_policy(policy)
        intent = {"scope": scope, "scope_id": scope_id, "policy": normalized,
                  "inherit": inherit, "expected_revision": expected_revision,
                  "operation_id": operation_id, "actor": actor, "reason": reason}
        digest = rules.fingerprint(intent)
        with transaction(self.conn):
            return self._apply_in_tx(intent, digest)

    def inherit_new_task_in_tx(self, task_id: str, *, actor: str) -> None:
        """仅由create_task持写锁调用；默认变化不追溯旧Task，失败回滚Task创建。"""
        from .delivery_guard import check_delivery_guard

        if not self.conn.in_transaction:
            raise RuntimeError("新任务策略快照须与创建同事务")
        if not check_delivery_guard(self.conn):
            return  # S1草案不自动纳管
        project = self._task_project(task_id)
        if not self._snapshot("project", project, project)["managed"]:
            return
        intent = {"scope": "task", "scope_id": task_id, "policy": None, "inherit": True,
                  "expected_revision": 0, "operation_id": "task-create-policy:" + task_id,
                  "actor": actor, "reason": "new-task-project-default-snapshot"}
        self._apply_in_tx(intent, rules.fingerprint(intent))

    def _apply_in_tx(self, intent: dict, digest: str) -> dict:
        scope, scope_id = intent["scope"], intent["scope_id"]
        operation_id, actor, reason = intent["operation_id"], intent["actor"], intent["reason"]
        expected_revision, inherit = intent["expected_revision"], intent["inherit"]
        normalized = intent["policy"]
        project = scope_id if scope == "project" else self._task_project(scope_id)
        current = self._snapshot(scope, scope_id, project)
        operation_key = _key("operation", operation_id)
        old = self._load(operation_key)
        if old is not None:
            if (set(old) != {"schema", "operation_id", "fingerprint", "scope", "scope_id",
                            "project", "revision"}
                    or type(old["schema"]) is not int or old["schema"] != rules.SCHEMA
                    or old["operation_id"] != operation_id
                    or not isinstance(old["fingerprint"], str) or len(old["fingerprint"]) != 64
                    or old["scope"] not in ("project", "task")
                    or type(old["revision"]) is not int or old["revision"] < 1):
                _corrupt("策略操作索引非法")
            original = self._version(old["scope"], old["scope_id"], old["project"], old["revision"])
            if original["operation_id"] != operation_id:
                _corrupt("策略操作与版本不匹配")
            if old["fingerprint"] != digest:
                raise LedgerError("DELIVERY_POLICY_OPERATION_CONFLICT", "同操作ID携带不同请求")
            if (old["scope"], old["scope_id"], old["project"]) != (scope, scope_id, project):
                _corrupt("策略操作归属不匹配")
            return {"ok": True, "applied": False, "operation_id": operation_id,
                    "applied_revision": old["revision"], "current": current,
                    "execution_enforced": current["execution_enforced"]}
        if current["revision"] != expected_revision:
            raise LedgerError("DELIVERY_POLICY_REVISION_CONFLICT", "策略版本已变化",
                              {"expected": expected_revision, "actual": current["revision"]})
        source = None
        if inherit:
            default = self._snapshot("project", project, project)
            if not default["managed"]:
                raise LedgerError("DELIVERY_POLICY_DEFAULT_MISSING", "项目默认尚未配置")
            normalized = default["version"]["policy"]
            source = {"scope": "project", "scope_id": project, "revision": default["revision"]}
        new_revision = current["revision"] + 1
        record = {"schema": rules.SCHEMA, "scope": scope, "scope_id": scope_id,
                  "project": project, "revision": new_revision, "policy": normalized,
                  "source": source, "actor": actor, "reason": reason,
                  "operation_id": operation_id, "created_at": self.clock.now_ms()}
        rules.validate_version(record, scope=scope, scope_id=scope_id,
                               project=project, expected_revision=new_revision)
        self._insert(_key("version", scope, scope_id, new_revision), record)
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (_key("head", scope, scope_id), rules.canonical({key: record[key] for key in
                ("schema", "scope", "scope_id", "project", "revision")})))
        self._insert(operation_key, {"schema": rules.SCHEMA, "operation_id": operation_id,
            "fingerprint": digest, "scope": scope, "scope_id": scope_id,
            "project": project, "revision": new_revision})
        self.conn.execute(
            "INSERT INTO audit(at,actor,action,entity,entity_id,from_state,to_state,detail) "
            "VALUES(?,?,?,?,?,?,?,?)", (record["created_at"], actor, "delivery-policy.draft-switch",
            scope, scope_id, str(current["revision"]), str(new_revision), rules.canonical({
                "operation_id": operation_id, "project": project, "source": source,
                "execution_enforced": current["coverage"]["writer_guard"]})))
        new_current = self._snapshot(scope, scope_id, project)
        return {"ok": True, "applied": True, "operation_id": operation_id,
                "applied_revision": new_revision,
                "current": new_current,
                "execution_enforced": new_current["execution_enforced"]}

"""独立资源授权：meta/audit 内不可变版本、CAS/操作幂等与只读授权检查。

命名空间 resource-authorization:v1:（JSON 数组 key 避免碰撞）。本底座只记录
显式授权事实并提供只读检查；它本身不产生单次动作许可，也不代表全路径费用
受控。snapshot/authorize 只在同一 WAL 快照里读取并校验，绝不写库、不 DDL、
不读模型目录或价格来源猜测免费属性；已开启事务时直接 join（由上层在同一
BEGIN IMMEDIATE 内组装单次许可与模式切换）。
"""

from __future__ import annotations

import json
import sqlite3

from ..domain import resource_authorization as rules
from ..domain.errors import LedgerError
from .sqlite_db import read_snapshot, transaction

PREFIX = "resource-authorization:v1:"


def _corrupt(message: str):
    raise LedgerError(rules.CODE_CORRUPT, message)


def _key(kind: str, *identity) -> str:
    return PREFIX + kind + ":" + rules.canonical(identity)


class ResourceAuthorizationStore:
    def __init__(self, conn: sqlite3.Connection, clock):
        self.conn, self.clock = conn, clock

    # ---- meta 原语 --------------------------------------------------
    def _load(self, key):
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row is None:
            return None
        try:
            value = json.loads(row[0])
        except (TypeError, ValueError) as exc:
            raise LedgerError(rules.CODE_CORRUPT, "授权JSON损坏") from exc
        if not isinstance(value, dict):
            _corrupt("授权记录不是对象")
        return value

    def _insert(self, key, value):
        self.conn.execute("INSERT INTO meta(key,value) VALUES(?,?)",
                          (key, rules.canonical(value)))

    # ---- 归属解析 --------------------------------------------------
    def _task_project(self, task_id: str):
        rules.identity(task_id, "task_id")
        row = self.conn.execute("SELECT project FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise LedgerError(rules.CODE_TASK_MISSING, "任务不存在", {"task_id": task_id})
        return rules.identity(row[0], "project")

    def _resolve_project(self, scope: str, scope_id: str):
        if scope == "project":
            rules.identity(scope_id, "project")
            return scope_id
        if scope == "task":
            rules.identity(scope_id, "task_id")
            return self._task_project(scope_id)
        # coordinator：显式 session 身份，不猜共享父模型/不取 project
        rules.identity(scope_id, "coordinator_session")
        return None

    # ---- 读取（只读；坏状态失败关闭） ---------------------------------
    def _version(self, scope, scope_id, project, revision):
        record = self._load(_key("version", scope, scope_id, revision))
        if record is None:
            _corrupt("授权head/operation引用的版本缺失")
        try:
            record = rules.validate_version(record, scope=scope, scope_id=scope_id,
                                            project=project, expected_revision=revision)
        except LedgerError as exc:
            if exc.code == rules.CODE_INVALID:
                raise LedgerError(rules.CODE_CORRUPT, "持久授权版本非法") from exc
            raise
        expected_operation = {
            "schema": rules.SCHEMA, "operation_id": record["operation_id"],
            "scope": scope, "scope_id": scope_id, "project": project,
            "revision": revision,
            "fingerprint": rules.fingerprint({
                "scope": scope, "scope_id": scope_id,
                "resources": record["resources"], "expires_at": record["expires_at"],
                "expected_revision": revision - 1,
                "operation_id": record["operation_id"],
                "actor": record["actor"], "reason": record["reason"]})}
        operation = self._load(_key("operation", record["operation_id"]))
        if (operation != expected_operation
                or type(operation.get("schema")) is not int
                or type(operation.get("revision")) is not int):
            _corrupt("授权版本与操作索引缺失或不一致")
        return record

    def _snapshot(self, scope, scope_id, project):
        head = self._load(_key("head", scope, scope_id))
        version = None
        if head is not None:
            if (set(head) != {"schema", "scope", "scope_id", "project", "revision"}
                    or type(head["schema"]) is not int or head["schema"] != rules.SCHEMA
                    or head["scope"] != scope or head["scope_id"] != scope_id
                    or head["project"] != project or type(head["revision"]) is not int
                    or head["revision"] < 1):
                _corrupt("授权head身份/schema/revision非法")
            version = self._version(scope, scope_id, project, head["revision"])
        else:
            # 不能把被误删的head读成从未纳管。
            prefix = _key("version", scope, scope_id)[:-1] + ","
            orphan = self.conn.execute(
                "SELECT 1 FROM meta WHERE key>=? AND key<? LIMIT 1",
                (prefix, prefix + "\uffff")).fetchone()
            if orphan is not None:
                _corrupt("已有授权版本但head缺失")
        expires_at = version["expires_at"] if version else None
        expired = version is not None and self.clock.now_ms() >= expires_at
        return {"ok": True, "schema": "oc-orch.resource-authorization.v1",
                "scope": scope, "scope_id": scope_id, "project": project,
                "managed": version is not None,
                "revision": version["revision"] if version else 0,
                "version": version,
                 "resources": version["resources"] if version else [],
                 "expires_at": expires_at, "expired": expired,
                 "revoked": version is not None and not version["resources"],
                 "valid": version is not None and bool(version["resources"]) and not expired}

    def snapshot(self, scope: str, scope_id: str) -> dict:
        if scope not in rules.SCOPES:
            rules.invalid("未知scope")
        with read_snapshot(self.conn):
            return self._snapshot(scope, scope_id, self._resolve_project(scope, scope_id))

    def authorize(self, scope: str, scope_id: str, *, model: str, preset: str,
                  now: int | None = None) -> dict:
        """只读授权检查，不写库、不产生单次许可。任何拒绝抛稳定码。"""
        if scope not in rules.SCOPES:
            rules.invalid("未知scope")
        checked_at = self.clock.now_ms() if now is None else now
        rules.validate_check_args(model=model, preset=preset, now_ms=checked_at)
        with read_snapshot(self.conn):
            project = self._resolve_project(scope, scope_id)
            state = self._snapshot(scope, scope_id, project)
            if not state["managed"]:
                raise LedgerError(rules.CODE_MISSING, "该scope尚未配置资源授权",
                                  {"scope": scope, "scope_id": scope_id})
            version = state["version"]
            if not version["resources"]:
                raise LedgerError(rules.CODE_REVOKED, "资源授权已撤销",
                                  {"scope": scope, "scope_id": scope_id,
                                   "revision": version["revision"]})
            outcome = rules.check_authorization(
                version["resources"], model=model, preset=preset,
                now_ms=checked_at, expires_at=version["expires_at"])
            entry = outcome["entry"]
            return {"ok": True, "allowed": True,
                    "scope": scope, "scope_id": scope_id, "project": project,
                    "authorization_revision": version["revision"],
                    "expires_at": version["expires_at"],
                    "resource": {"model": entry["model"], "cost": entry["cost"],
                                 "evidence": entry["evidence"],
                                 "internal_calls_controlled": entry["internal_calls_controlled"]},
                    "preset": preset, "checked_at": checked_at,
                    "single_use_permit": False}

    # ---- 写入（短事务/CAS/操作幂等；与head/audit同一BEGIN IMMEDIATE） ---
    def replace(self, scope: str, scope_id: str, *, resources: list, expires_at: int,
                expected_revision: int, operation_id: str, actor: str,
                reason: str) -> dict:
        if scope not in rules.SCOPES:
            rules.invalid("未知scope")
        for label, value in (("operation_id", operation_id), ("actor", actor), ("reason", reason)):
            rules.identity(value, label)
        rules.revision(expected_revision)
        if type(expires_at) is not int or expires_at < 0:
            rules.invalid("expires_at必须为非负整数毫秒时间戳")
        normalized = rules.normalize_resources(resources)
        intent = {"scope": scope, "scope_id": scope_id, "resources": normalized,
                  "expires_at": expires_at, "expected_revision": expected_revision,
                  "operation_id": operation_id, "actor": actor, "reason": reason}
        digest = rules.fingerprint(intent)
        with transaction(self.conn):
            project = self._resolve_project(scope, scope_id)
            now_ms = self.clock.now_ms()
            current = self._snapshot(scope, scope_id, project)
            operation_key = _key("operation", operation_id)
            old = self._load(operation_key)
            if old is not None:
                if (set(old) != {"schema", "operation_id", "fingerprint", "scope", "scope_id",
                                 "project", "revision"}
                        or type(old["schema"]) is not int or old["schema"] != rules.SCHEMA
                        or old["operation_id"] != operation_id
                        or not isinstance(old["fingerprint"], str) or len(old["fingerprint"]) != 64
                        or old["scope"] not in rules.SCOPES
                        or type(old["revision"]) is not int or old["revision"] < 1):
                    _corrupt("授权操作索引非法")
                original = self._version(old["scope"], old["scope_id"],
                                         old["project"], old["revision"])
                if original["operation_id"] != operation_id:
                    _corrupt("授权操作与版本不匹配")
                if old["fingerprint"] != digest:
                    raise LedgerError(rules.CODE_OPERATION, "同操作ID携带不同授权请求")
                if (old["scope"], old["scope_id"], old["project"]) != (scope, scope_id, project):
                    _corrupt("授权操作归属不匹配")
                # 幂等回放：返回原applied_revision，current是当前head，分开给调用方。
                return {"ok": True, "applied": False, "operation_id": operation_id,
                        "applied_revision": old["revision"], "current": current,
                        "single_use_permit": False}
            # 历史同体回放只报告原结果，过期不应使其冒充新授予或破坏幂等。
            rules.future_expiry(expires_at, now_ms)
            if current["revision"] != expected_revision:
                raise LedgerError(rules.CODE_REVISION, "资源授权版本已变化",
                                  {"expected": expected_revision, "actual": current["revision"]})
            new_revision = current["revision"] + 1
            record = {"schema": rules.SCHEMA, "scope": scope, "scope_id": scope_id,
                      "project": project, "revision": new_revision, "resources": normalized,
                      "expires_at": expires_at, "actor": actor, "reason": reason,
                      "operation_id": operation_id, "created_at": now_ms}
            rules.validate_version(record, scope=scope, scope_id=scope_id,
                                   project=project, expected_revision=new_revision)
            self._insert(_key("version", scope, scope_id, new_revision), record)
            self.conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (_key("head", scope, scope_id),
                 rules.canonical({key: record[key] for key in
                                  ("schema", "scope", "scope_id", "project", "revision")})))
            self._insert(operation_key, {"schema": rules.SCHEMA, "operation_id": operation_id,
                                         "fingerprint": digest, "scope": scope,
                                         "scope_id": scope_id, "project": project,
                                         "revision": new_revision})
            self.conn.execute(
                "INSERT INTO audit(at,actor,action,entity,entity_id,from_state,to_state,detail) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (now_ms, actor, "resource-authorization.replace", scope, scope_id,
                 str(current["revision"]), str(new_revision),
                 rules.canonical({"operation_id": operation_id, "project": project,
                                  "resources": normalized, "expires_at": expires_at,
                                  "single_use_permit": False})))
            return {"ok": True, "applied": True, "operation_id": operation_id,
                    "applied_revision": new_revision,
                    "current": self._snapshot(scope, scope_id, project),
                    "single_use_permit": False}

"""Task原型权威记录：meta内CAS/幂等/不可变历史，正式验收引用不替代verify。"""
import json

from .sqlite_db import transaction, read_snapshot
from ..domain import prototype as rules
from ..domain.delivery_policy import identity, revision, fingerprint, canonical
from ..domain.errors import LedgerError

PREFIX = "prototype-plan:v1:"


class PrototypeStore:
    def __init__(self, ledger, clock):
        self.ledger, self.conn, self.clock = ledger, ledger.conn, clock

    def _load(self, task_id):
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (PREFIX + task_id,)).fetchone()
        if row is None:
            return None
        try:
            record = json.loads(row[0])
            digest = record.pop("integrity")
            if (set(record) != {"schema", "task_id", "commitment", "versions"}
                    or digest != fingerprint(record) or record["schema"] != 1
                    or type(record["schema"]) is not int or record["task_id"] != task_id
                    or set(record["commitment"]) != {"objective", "acceptance"}):
                raise ValueError("integrity/identity")
            if not isinstance(record["versions"], list) or not 1 <= len(record["versions"]) <= 128:
                raise ValueError("versions")
            operations = set()
            for number, version in enumerate(record["versions"], 1):
                if (set(version) != {"revision", "plan", "operation_id", "reason", "actor", "created_at", "intent_hash"}
                        or type(version["revision"]) is not int or version["revision"] != number
                        or type(version["created_at"]) is not int or version["created_at"] < 0):
                    raise ValueError("revision")
                for key in ("operation_id", "reason", "actor", "intent_hash"):
                    identity(version[key], key)
                if version["operation_id"] in operations:
                    raise ValueError("duplicate operation")
                operations.add(version["operation_id"])
                rules.normalize(version["plan"])
                if number > 1:
                    rules.preserve(record["versions"][number - 2]["plan"], version["plan"])
            return record
        except Exception as exc:
            raise LedgerError("PROTOTYPE_CORRUPT", "权威原型记录损坏，不能当作无缺口") from exc

    def _show(self, task_id, operation_id=None):
        task = self.ledger.get_task(task_id)
        record = self._load(task_id)
        current = record["versions"][-1] if record else None
        receipt = next((v for v in record["versions"] if v["operation_id"] == operation_id), None) if record and operation_id else None
        plan = current["plan"] if current else None
        gaps = plan["gaps"] if plan else []
        unresolved = [g["id"] for g in gaps if g["state"] != "fixed"]
        return {"ok": True, "schema": "oc-orch.prototype.v1", "task_id": task_id, "project": task["project"],
                "revision": current["revision"] if current else 0, "current": current,
                "commitment": record["commitment"] if record else {"objective": task["objective"], "acceptance": task["acceptance"]},
                "unresolved": unresolved, "applied_revision": receipt["revision"] if receipt else None,
                "semantics": "prototype-scope-not-formal-business-acceptance",
                "traceability": {"features_requiring_gaps": sum(f["state"] != "real" for f in plan["features"]) if plan else 0,
                                 "unresolved_gaps": len(unresolved), "recording_is_not_repair": True}}

    def show(self, task_id, operation_id=None):
        identity(task_id, "task_id")
        if operation_id is not None:
            identity(operation_id, "operation_id")
        with read_snapshot(self.conn):
            return self._show(task_id, operation_id)

    def apply(self, task_id, body, *, actor):
        if not isinstance(body, dict) or set(body) != {"plan", "expected_revision", "operation_id", "reason", "confirm"}:
            rules.reject("只接受plan/CAS/operation/reason/逐字confirm")
        identity(task_id, "task_id")
        identity(actor, "actor")
        for key in ("operation_id", "reason"):
            identity(body[key], key)
        revision(body["expected_revision"])
        if body["confirm"] != task_id:
            rules.reject("须逐字确认Task")
        plan = rules.normalize(body["plan"])
        intent_hash = fingerprint({**body, "actor": actor})
        with transaction(self.conn):
            task = self.ledger.get_task(task_id)
            record = self._load(task_id)
            versions = record["versions"] if record else []
            for old in versions:
                if old["operation_id"] == body["operation_id"]:
                    if old["intent_hash"] != intent_hash:
                        raise LedgerError("PROTOTYPE_OPERATION_CONFLICT", "原操作不得换内容")
                    return {**self._show(task_id, body["operation_id"]), "applied": False}
            if body["expected_revision"] != len(versions):
                raise LedgerError("PROTOTYPE_REVISION_CONFLICT", "原型记录版本已变化")
            if len(versions) >= 128:
                rules.reject("历史已达128版本；不丢历史，需显式归档方案")
            if versions:
                rules.preserve(versions[-1]["plan"], plan)
            for gap in plan["gaps"]:
                if gap["followup_task"] is not None:
                    followup = self.ledger.get_task(gap["followup_task"])
                    if followup["project"] != task["project"]:
                        rules.reject("后续Task须属于同项目；不借记录扩大权限或改变项目归属")
                proof = gap["evidence"]
                if gap["state"] != "fixed":
                    continue
                att = self.ledger.get_attempt(proof["attempt_id"])
                followup = self.ledger.get_task(att["task_id"])
                verdict = att.get("verification") or {}
                if (att["task_id"] != (gap["followup_task"] or task_id) or followup["project"] != task["project"]
                        or att["status"] != "accepted" or verdict.get("classification") != "normal_effective"
                        or verdict.get("result") != "pass" or verdict.get("verified_at") != proof["verified_at"]):
                    rules.reject("缺口修复必须引用本Task/显式同项目后续Task的正常有效验收，拒绝force/聊天回执")
            previous_revision = len(versions)
            version = {"revision": previous_revision + 1, "plan": plan, "operation_id": body["operation_id"],
                       "reason": body["reason"], "actor": actor, "created_at": self.clock.now_ms(), "intent_hash": intent_hash}
            record = record or {"schema": 1, "task_id": task_id, "commitment": {
                "objective": task["objective"], "acceptance": task["acceptance"]}, "versions": []}
            record["versions"].append(version)
            record["integrity"] = fingerprint(record)
            self.conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                              (PREFIX + task_id, canonical(record)))
            self.ledger._audit(self.conn, actor, "prototype.update", "task", task_id, str(previous_revision), str(version["revision"]),
                               {"operation_id": body["operation_id"], "unresolved": sum(g["state"] != "fixed" for g in plan["gaps"])})
            return {**self._show(task_id, body["operation_id"]), "applied": True}

"""显式用户输入持久意图与独立协调者许可；无DDL、不变更项目暂停/任务/验收。"""
import json

from .coordinator_control import CoordinatorControlLedger
from .delivery_action import DeliveryActions
from .sqlite_db import transaction, read_snapshot
from ..domain.delivery_policy import canonical, fingerprint, identity
from ..domain.errors import LedgerError
from ..domain.operator_turn import validate

PREFIX = "operator-turn:v1:"


class OperatorTurnStore:
    def __init__(self, ledger, clock):
        self.ledger, self.conn, self.clock = ledger, ledger.conn, clock
        self.control = CoordinatorControlLedger(self.conn, clock)

    def get(self, operation_id):
        identity(operation_id, "operation_id")
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (PREFIX + operation_id,)).fetchone()
        if row is None:
            return None
        try:
            record = json.loads(row[0])
            validate(record["intent"])
            immutable = {k: record[k] for k in ("schema", "intent", "message_id", "origin_at", "permit")}
            if (type(record["schema"]) is not int or record["schema"] != 1
                    or record["intent"]["operation_id"] != operation_id
                    or record["integrity"] != fingerprint(immutable)
                    or record["state"] not in ("possible", "admitted", "unknown", "finished")
                    or type(record["origin_at"]) is not int
                    or (record["state"] == "finished" and not isinstance(record.get("completion"), dict))):
                raise ValueError("invalid intent")
            return record
        except Exception as exc:
            raise LedgerError("OPERATOR_TURN_CORRUPT", "显式输入意图损坏，不猜未发送、不重投") from exc

    def matching(self, body):
        record = self.get(body["operation_id"])
        if record and record["intent"] != body:
            raise LedgerError("OPERATOR_TURN_CONFLICT", "同operation不得换正文/作用域/版本")
        return record

    def authority(self, body):
        lease = self.ledger.coordinator_lease(body["project"])
        if (not lease or lease["holder_session"] != body["session_id"]
                or lease["epoch"] != body["expected_epoch"] or lease["expires_at"] <= self.clock.now_ms()
                or lease["recovery_blocked"]):
            raise LedgerError("OPERATOR_TURN_AUTHORITY", "租约缺失/过期/旧holder，不自动接管")

    def prepare(self, body, observation):
        with transaction(self.conn):
            old = self.matching(body)
            if old:
                return old, False
            self.authority(body)
            snap = self.control.get_session(body["session_id"])
            if snap["revision"] != body["expected_revision"]:
                raise LedgerError("OPERATOR_TURN_REVISION", "回合版本已变化")
            actions = DeliveryActions(self.ledger, self.clock)
            if not actions.enabled():
                raise LedgerError("OPERATOR_TURN_NOT_ENABLED", "须显式启用完整delivery契约；不回落旧发送路径")
            sid, model = body["session_id"], observation.get("model")
            if observation.get("session_id") != sid:
                raise LedgerError("DELIVERY_MODEL_UNKNOWN", "session模型观察身份不符")
            actions._provider(model)
            actions._check_target_gate(sid)
            authorization = actions._authorization(None, model, coordinator=sid)
            mid = "msg_ocorchUser" + fingerprint(body["operation_id"])[:24]
            if self.ledger.input_for_receipt(sid, mid):
                raise LedgerError("OPERATOR_TURN_AUTOMATIC", "原ID已有受管输入身份，不能改登记用户")
            now = self.clock.now_ms()
            result = self.control.begin_user_turn_in_tx(sid, message_id=mid, origin_at=now,
                operation_id=body["operation_id"], trusted=True)
            if not result.get("executed"):
                raise LedgerError("OPERATOR_TURN_REFUSED", result.get("refused", "unknown"))
            permit = actions._issue(session_id=sid, message_id=mid, operation="prompt", input_id=None,
                attempt_id=None, task_id=None, policy=None, model=model, authorization=authorization,
                work={"kind": "partition", "reason": "explicit-user-conversation", "round": 0}, body_hash=fingerprint(body["text"]))
            immutable = {"schema": 1, "intent": body, "message_id": mid, "origin_at": now, "permit": permit}
            record = {**immutable, "integrity": fingerprint(immutable), "state": "possible", "completion": None}
            self.conn.execute("INSERT INTO meta(key,value) VALUES(?,?)", (PREFIX + body["operation_id"], canonical(record)))
            self.ledger._audit(self.conn, "operator", "operator-turn.possible", "session", sid, None, "possible",
                                {"operation_id": body["operation_id"], "message_id": mid, "source": "explicit-operator"})
            return record, True

    def mark(self, operation_id, state, completion=None):
        with transaction(self.conn):
            return self._mark_in_tx(operation_id, state, completion)

    def _mark_in_tx(self, operation_id, state, completion=None):
        record = self.get(operation_id)
        if record["state"] != "finished":
            record.update(state=state, completion=completion)
            self.conn.execute("UPDATE meta SET value=? WHERE key=?", (canonical(record), PREFIX + operation_id))
        return record

    def finish_registered(self, session_id, *, operation_id, evidence, **kw):
        with transaction(self.conn):
            result = self.control.finish_user_turn_in_tx(session_id, **kw)
            if result.get("executed"):
                record = self.get(operation_id)
                if record and record["message_id"] == kw["message_id"]:
                    self._mark_in_tx(operation_id, "finished", evidence)
            return result

    def show(self, project, session_id, operation_id):
        for key, value in (("project", project), ("session_id", session_id), ("operation_id", operation_id)):
            identity(value, key)
        with read_snapshot(self.conn):
            record = self.get(operation_id)
            if record and (record["intent"]["project"], record["intent"]["session_id"]) != (project, session_id):
                raise LedgerError("OPERATOR_TURN_SCOPE", "回执不属于该项目/session")
            return {"ok": True, "operation": ({"operation_id": operation_id, "project": project,
                "session_id": session_id, "message_id": record["message_id"], "origin_at": record["origin_at"],
                "state": record["state"], "source": "explicit-operator", "completion": record["completion"]} if record else None),
                "session": self.control.get_session(session_id), "project": self.control.get_project(project)}

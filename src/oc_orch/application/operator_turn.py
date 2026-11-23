"""指定显式入口默认用户的提交/对账；不扫普通聊天，不将HTTP来源字段视作认证。"""
from ..domain.operator_turn import validate
from ..domain.delivery_action import observed_model, check_handoff
from ..domain.errors import LedgerError


class OperatorTurnService:
    def __init__(self, store, observer, *, client, clock):
        self.store, self.observer, self.client, self.clock = store, observer, client, clock

    def show(self, *, project, session_id, operation_id):
        return self.store.show(project, session_id, operation_id)

    def send(self, body):
        body = validate(body)
        old = self.store.matching(body)
        if old:
            return {**self.show(project=body["project"], session_id=body["session_id"], operation_id=body["operation_id"]), "duplicate": True}
        self.store.authority(body)
        client = self.client()
        sid = body["session_id"]
        observation = observed_model(client.get_session(sid), sid)
        if client.inbox(sid):
            raise LedgerError("OPERATOR_TURN_QUEUE", "队列非空，不借resume消费未知/自动输入")
        record, created = self.store.prepare(body, observation)
        if created:
            try:
                check_handoff(record["permit"], self.clock.now_ms())
                client.prompt(sid, message_id=record["message_id"], text=body["text"], delivery="queue", resume=True)
            except Exception:
                self.store.mark(body["operation_id"], "unknown")
            else:
                self.store.mark(body["operation_id"], "admitted")
        return {**self.show(project=body["project"], session_id=sid, operation_id=body["operation_id"]), "duplicate": not created}

    def observe(self, *, project, session_id, operation_id):
        view = self.show(project=project, session_id=session_id, operation_id=operation_id)
        record = self.store.get(operation_id)
        if not record:
            return {**view, "finished": False, "reason": "unknown-operation"}
        if record["state"] == "finished":
            return {**view, "finished": True, "reason": "already-observed"}
        if (view["session"].get("barrier") or {}).get("message_id") != record["message_id"]:
            return {**view, "finished": False, "reason": "superseded-or-unattributed"}
        self.store.authority(record["intent"])
        result = self.observer.observe(session_id)
        if result["finished"] and result["message_id"] == record["message_id"]:
            self.store.mark(operation_id, "finished", result)
        else:
            result = {"finished": False, "reason": result.get("reason", "superseded")}
        return {**self.show(project=project, session_id=session_id, operation_id=operation_id), **result}

"""单次发送许可的纯值与最后交接检查；不把许可当模型执行/验收证据。"""

from .delivery_policy import identity
from .errors import LedgerError

HANDOFF_MS = 30_000


def observed_model(session, session_id: str) -> dict:
    """只采用scoped REST当前session的明确模型，不猜default/目录价格。"""
    if not isinstance(session, dict) or session.get("id") != session_id:
        raise LedgerError("DELIVERY_MODEL_UNKNOWN", "当前session/模型观察身份未知")
    model = session.get("model")
    if not isinstance(model, dict):
        raise LedgerError("DELIVERY_MODEL_UNKNOWN", "session未报告明确模型；拒绝资源动作")
    provider, model_id = model.get("providerID"), model.get("id")
    identity(provider, "providerID")
    identity(model_id, "modelID")
    if "/" in provider:
        raise LedgerError("DELIVERY_MODEL_UNKNOWN", "模型providerID不合法")
    return {"model": provider + "/" + model_id, "session_id": session_id}


def collaboration(value) -> dict:
    """声明式协作预算：分工、完整候选和可选挑战分别计数，不解析自然语言。"""
    if value is None:
        return {"kind": "partition", "reason": "single-route-continuity", "round": 0}
    if (not isinstance(value, dict) or set(value) != {"kind", "reason", "round"}
            or value["kind"] not in ("partition", "candidate", "challenge")
            or type(value["round"]) is not int or value["round"] < 0):
        raise LedgerError("DELIVERY_COLLABORATION_INVALID", "协作声明须为kind/reason/round")
    identity(value["reason"], "collaboration.reason")
    if value["kind"] != "challenge" and value["round"] != 0:
        raise LedgerError("DELIVERY_COLLABORATION_INVALID", "非challenge不能声明挑战回合")
    if value["kind"] == "challenge" and value["round"] < 1:
        raise LedgerError("DELIVERY_COLLABORATION_INVALID", "挑战回合须从1开始")
    return dict(value)


def check_handoff(permit: dict | None, now: int) -> None:
    """不可缓存许可；过期只保原身份待人工/对账，不自动续旧版本。"""
    if permit is None:
        return
    if (type(now) is not int or now < permit["issued_at"]
            or now >= permit["expires_at"]):
        raise LedgerError("DELIVERY_ACTION_EXPIRED", "单次发送交接窗已过期；不发送/不自动续期",
                          {"session_id": permit["session_id"], "message_id": permit["message_id"]})

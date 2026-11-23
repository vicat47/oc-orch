"""所有受管发送共用最后许可边界；REST观察/模型请求都在写事务外。"""

from ..domain.delivery_action import check_handoff, observed_model
from ..domain.errors import LedgerError


def model_observation(ledger, client, session_id):
    if not ledger.delivery_policy_enabled():
        return None
    try:
        return observed_model(client.get_session(session_id), session_id)
    except Exception as exc:
        raise LedgerError("DELIVERY_MODEL_UNKNOWN", "发送前无法确认当前session模型，拒绝新许可") from exc


def authorize_attempt(ledger, client, clock, attempt_id):
    att = ledger.get_attempt(attempt_id)
    observation = (model_observation(ledger, client, att["session_id"])
                   if ledger.delivery_attempt_required(attempt_id) else {})
    # 是否需要模型观察是优化，不是放行条件；写锁内仍重读当前策略/契约，
    # 若观察期间刚纳管则无模型证据会拒绝，不能因先前未纳管绕过新策略。
    permit = ledger.authorize_delivery_attempt(attempt_id, observation=observation)
    check_handoff(permit, clock.now_ms())
    return permit


def authorize_input(ledger, client, clock, row, *, operation, owner):
    observation = (model_observation(ledger, client, row["session_id"])
                   if ledger.delivery_input_required(row["input_id"]) else {})
    permit = ledger.authorize_delivery_input(row["input_id"], observation=observation,
        operation=operation, owner=owner, expected_revision=row["revision"])
    check_handoff(permit, clock.now_ms())
    return permit

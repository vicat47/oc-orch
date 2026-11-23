"""指定显式提交的严格意图；本地operator约定不是通用HTTP真人认证。"""
from .delivery_policy import identity, revision
from .errors import LedgerError


def validate(body):
    fields = {"project", "session_id", "operation_id", "text", "reason", "expected_revision", "expected_epoch", "confirm"}
    if not isinstance(body, dict) or set(body) != fields:
        raise LedgerError("OPERATOR_TURN_INVALID", "只接受显式提交字段；不接收来源/消息ID/结束证据提权")
    for key in ("project", "session_id", "operation_id", "reason"):
        identity(body[key], key)
    revision(body["expected_revision"])
    if type(body["expected_epoch"]) is not int or body["expected_epoch"] < 1:
        raise LedgerError("OPERATOR_TURN_INVALID", "expected_epoch须为正整数")
    if body["confirm"] != body["project"]:
        raise LedgerError("CONFIRM_REQUIRED", "须逐字确认项目")
    if not isinstance(body["text"], str) or not body["text"].strip() or len(body["text"].encode()) > 100_000:
        raise LedgerError("OPERATOR_TURN_INVALID", "正文须非空且不超过100KB")
    return dict(body)

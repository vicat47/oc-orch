"""副作用前的本地控制守卫；调用方可在已有短写事务内复核。

不创建控制记录、不做网络；未知/损坏一律拒绝，不把暂停变成取消。
"""

from ..domain.coordinator_control import CoordinatorControlStateError
from ..domain.errors import LedgerError
from ..util import json_loads
from .clock import SystemClock
from .coordinator_control import CoordinatorControlLedger


def _check_dependency_review(conn, clock, notice: dict) -> None:
    """发送时重查上游证据；只读适配器不访问结果文件或网络。"""
    metadata, payload = notice.get("metadata"), notice.get("payload")
    if isinstance(metadata, str):
        metadata = json_loads(metadata, {})
    if isinstance(payload, str):
        payload = json_loads(payload, {})
    metadata = metadata if isinstance(metadata, dict) else {}
    payload = payload if isinstance(payload, dict) else {}
    if not (metadata.get("condition_identity")
            or payload.get("classification") == "DEPENDENCY_SATISFIED"):
        return
    # 此窄能力仅查询SQLite。新Ledger不读home、不读配置、不执行外部I/O。
    # 惰性导入避免Ledger/InputLedger/守卫的模块初始化循环。
    from .sqlite_ledger import Ledger
    from .responsibility_ledger import ResponsibilityStore

    store = ResponsibilityStore(Ledger(conn, "", clock=clock))
    store.check_dependency_review_notification(
        {**notice, "metadata": metadata, "payload": payload})


def check_project_dispatch(conn, clock, project: str) -> None:
    """worker 新派单只检查项目意图，不把用户回合屏障扩大为 worker 停止。"""
    try:
        snap = CoordinatorControlLedger(conn, clock or SystemClock()).get_project(project)
    except CoordinatorControlStateError as exc:
        raise LedgerError("CONTROL_STATE_UNKNOWN", "项目控制状态未知，拒绝新派单") from exc
    if snap["paused"]:
        raise LedgerError("PROJECT_PAUSED", "项目已显式暂停，拒绝新派单",
                          {"project": project, "revision": snap["revision"]})


def check_dispatch_authority(conn, clock, project: str, session_id: str | None,
                             *, expected: dict | None = None) -> dict:
    """派单主体与租约的本地快照；调用方在认领事务内复核并冻结。

    无租约的显式历史CLI仍可用；一旦有租约，就不能以--coordinator改绑旧
    holder绕过交接。lease_event来自既有claim/takeover/release审计，防止
    release后同毫秒重领导致epoch重置的ABA；普通续约不改变该身份。
    这是本地操作归属守卫，不声称session_id自报可证明真人授权。
    """
    check_project_dispatch(conn, clock, project)
    lease = conn.execute(
        "SELECT holder_session,epoch,expires_at,recovery_blocked "
        "FROM coordinator_leases WHERE project=?", (project,)).fetchone()
    if lease:
        if (not isinstance(lease["holder_session"], str) or not lease["holder_session"].strip()
                or type(lease["epoch"]) is not int or lease["epoch"] < 1
                or type(lease["expires_at"]) is not int or not 0 <= lease["expires_at"] <= 8 * 10**12
                or type(lease["recovery_blocked"]) is not int or lease["recovery_blocked"] not in (0, 1)):
            raise LedgerError("DISPATCH_AUTHORITY_INVALID", "租约字段损坏，拒绝新派单")
        if lease["holder_session"] != session_id:
            raise LedgerError("COORDINATOR_ROUTE_CHANGED", "派单主体不是当前租约holder")
        if lease["recovery_blocked"]:
            raise LedgerError("COORDINATOR_BLOCKED", "协调者已停用或恢复被阻断")
        if lease["expires_at"] <= (clock or SystemClock()).now_ms():
            raise LedgerError("COORDINATOR_LEASE_EXPIRED", "协调租约已过期；须先显式续约")
    event = conn.execute(
        "SELECT id FROM audit WHERE entity='coordinator' AND entity_id=? "
        "AND action IN ('coordinator.claim','coordinator.takeover','coordinator.release') "
        "ORDER BY id DESC LIMIT 1", (project,)).fetchone()
    snap = {"schema": 1, "project": project, "session_id": session_id,
            "epoch": lease["epoch"] if lease else None,
            "lease_event": event["id"] if event else None}
    if expected is not None:
        if (not isinstance(expected, dict) or type(expected.get("schema")) is not int
                or expected["schema"] != 1
                or any(v is not None and (type(v) is not int or v < 1)
                       for v in (expected.get("epoch"), expected.get("lease_event")))):
            raise LedgerError("DISPATCH_AUTHORITY_INVALID", "派单主体快照字段损坏，拒绝发送")
        if expected != snap:
            raise LedgerError("DISPATCH_AUTHORITY_CHANGED", "派单归属/租约代次已变化；保留原身份供核实")
    return snap


def _check_project_target(conn, clock, project: str, session_id: str) -> None:
    check_project_dispatch(conn, clock, project)
    lease = conn.execute(
        "SELECT holder_session FROM coordinator_leases WHERE project=?",
        (project,)).fetchone()
    if lease and lease["holder_session"] != session_id:
        raise LedgerError("COORDINATOR_ROUTE_CHANGED", "项目协调者已交接，拒绝唤醒旧目标",
                          {"project": project, "session_id": session_id})


def check_automatic_wake(conn, clock, session_id: str, notifications: list[dict]) -> None:
    """冻结批次逐项目复核；无作用域的历史通知在归属有歧义时保守拒发。"""
    control = CoordinatorControlLedger(conn, clock or SystemClock())
    try:
        session = control.get_session(session_id)
        if session["hold"] is not None or session["barrier"] is not None:
            raise LedgerError("COORDINATOR_HELD", "协调者回合 hold/用户屏障仍有效",
                              {"session_id": session_id, "revision": session["revision"]})
        for notice in notifications:
            _check_dependency_review(conn, clock, notice)
            project = notice.get("project")
            if not project and notice.get("task_id"):
                task = conn.execute("SELECT project FROM tasks WHERE id=?",
                                    (notice["task_id"],)).fetchone()
                project = task["project"] if task else None
            if project:
                _check_project_target(conn, clock, project, session_id)
                continue
            # 有项目暂停时不能把旧无作用域通知混进其他项目借道发送。
            # 明确属于其他运行项目的通知不受影响。
            projects = conn.execute(
                "SELECT project FROM coordinator_leases WHERE holder_session=? "
                "UNION SELECT project FROM tasks WHERE coordinator_session=?",
                (session_id, session_id)).fetchall()
            for row in projects:
                _check_project_target(conn, clock, row["project"], session_id)
    except CoordinatorControlStateError as exc:
        raise LedgerError("CONTROL_STATE_UNKNOWN", "协调控制状态未知，拒绝自动唤醒") from exc

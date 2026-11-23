"""显式登记回合的唯一结束观察器；入口和Supervisor共享完整原ID窗口及CAS。"""


class RegisteredTurnObserver:
    def __init__(self, ledger, control, observation, clock, *, client, window, finish):
        self.ledger, self.control, self.observation, self.clock = ledger, control, observation, clock
        self.client, self.window = client, window
        self.finish = finish

    def observe(self, session_id, session=None):
        def blocked(reason):
            return {"finished": False, "reason": reason}

        snap = self.control.get_session(session_id)
        barrier = snap.get("barrier")
        if not barrier or barrier.get("trusted") is not True:
            return blocked("no-registered-turn")
        hold = snap.get("hold")
        if hold and hold.get("reason") == "unattributed-interruption":
            return blocked("unattributed-interruption")
        client = self.client()
        session = session if session is not None else client.get_session(session_id)
        if not isinstance(session, dict):
            return blocked("session-unknown")
        mid, origin = barrier["message_id"], barrier["origin_at"]
        idle_at = (session.get("time") or {}).get("idle")
        if (session.get("id") != session_id or session.get("outcome") not in ("succeeded", "failed")
                or type(idle_at) is not int or not origin < idle_at <= self.clock.now_ms()):
            return blocked("terminal-not-attributed")
        original = self.ledger.input_for_receipt(session_id, mid)
        if original and original.get("source") == "automatic_wake":
            return blocked("known-automatic-input")
        claim = self.observation.claim(session_id, expected_revision=snap["revision"], idle_at=idle_at)
        if not claim["allowed"]:
            if claim["reason"] == "observation-exhausted":
                self.ledger.add_alert(severity="high", kind="user_turn_unknown", title="用户回合结束证据仍不足",
                    detail=f"session={session_id} message={mid}；保持原身份；next_action=人工核对，不自动重发",
                    dedupe_key=f"user-turn-unknown:{session_id}:{mid}")
            return blocked(claim["reason"])
        messages, coverage = self.window(session_id, anchor_id=mid)
        if coverage.get("complete") is not True:
            return blocked("incomplete-window")
        created = lambda message: (message.get("time") or {}).get("created")
        users = [m for m in messages if m.get("type") == "user"]
        idles = [m for m in messages if m.get("type") == "idle"]
        if (len(users) != 1 or users[0].get("id") != mid or type(created(users[0])) is not int
                or created(users[0]) < origin or len(idles) != 1):
            return blocked("ambiguous-user-window")
        idle = idles[0]
        if (not idle.get("id") or created(idle) != idle_at or idle_at <= created(users[0])
                or idle.get("outcome") != session.get("outcome")
                or any(m.get("type") in ("synthetic", "compaction", "shell", "skill") for m in messages)):
            return blocked("conflicting-window")
        latest = client.get_session(session_id)
        if (not latest or latest.get("id") != session_id or latest.get("outcome") != session.get("outcome")
                or (latest.get("time") or {}).get("idle") != idle_at
                or int((latest.get("time") or {}).get("busy") or 0) > idle_at
                or session_id in client.active_sessions() or client.inbox(session_id)):
            return blocked("latest-state-changed")
        evidence = {"finished": True, "reason": "original-window-verified", "message_id": mid,
                    "idle_id": idle["id"], "idle_at": idle_at, "coverage": coverage}
        result = self.finish(session_id, operation_id=barrier["operation_id"], evidence=evidence,
            message_id=mid, idle_id=idle["id"], idle_at=idle_at, expected_revision=snap["revision"])
        if result.get("executed") is not True:
            return blocked(result.get("refused", "finish-refused"))
        self.ledger.resurface_pending_notifications(session_id, next_attempt_at=self.clock.now_ms(),
            reason="registered-user-turn-finished", actor="supervisor")
        return evidence


def latest_user_message(client, session_id, *, limit=50):
    """读取最近消息中的最新一条用户输入；只返回身份/时间，不返回正文。"""
    items = client.messages(session_id, limit=limit, order="desc")
    for message in items or ():
        if message.get("type") != "user":
            continue
        created = (message.get("time") or {}).get("created")
        return {"message_id": message.get("id"), "created": created,
                "found": True}
    return {"found": False, "message_id": None, "created": None}


def verify_completed_user_turn(*, control, ledger, client, window, clock,
                               session_id, message_id):
    """核验一个已完成的真实用户回合，返回证据或稳定拒绝原因。

    只读；不写控制状态。调用方成功后才能把证据交给
    ``CoordinationControlService.register_completed_turn`` 在同一事务里
    begin+finish，避免失败留下半截 barrier。
    """
    def blocked(reason):
        return {"finished": False, "reason": reason, "message_id": message_id}

    try:
        snap = control.get_session(session_id)
    except Exception as exc:  # 控制状态损坏不得当作无 hold 放行
        return blocked(f"control-unknown:{type(exc).__name__}")
    if snap.get("barrier") is not None:
        return blocked("user-turn-active")
    hold = snap.get("hold")
    if not hold:
        return blocked("no-active-hold")
    if hold.get("reason") not in ("coordinator-interrupted",):
        return blocked("hold-not-releasable")
    original = ledger.input_for_receipt(session_id, message_id)
    if original and original.get("source") == "automatic_wake":
        return blocked("known-automatic-input")
    session = client.get_session(session_id)
    if not isinstance(session, dict) or session.get("id") != session_id:
        return blocked("session-unknown")
    outcome = session.get("outcome")
    idle_at = (session.get("time") or {}).get("idle")
    if (outcome not in ("succeeded", "failed")
            or type(idle_at) is not int or idle_at <= 0
            or idle_at > clock.now_ms()):
        return blocked("terminal-not-attributed")
    messages, coverage = window(session_id, anchor_id=message_id)
    if coverage.get("complete") is not True:
        return blocked("incomplete-window")
    created = lambda message: (message.get("time") or {}).get("created")
    users = [m for m in messages if m.get("type") == "user"]
    idles = [m for m in messages if m.get("type") == "idle"]
    if (len(users) != 1 or users[0].get("id") != message_id
            or type(created(users[0])) is not int):
        return blocked("ambiguous-user-window")
    origin_at = created(users[0])
    hold_origin = hold.get("origin_at")
    if type(hold_origin) is not int or origin_at <= hold_origin:
        return blocked("input-not-newer-than-hold")
    if len(idles) != 1:
        return blocked("ambiguous-user-window")
    idle = idles[0]
    if (not idle.get("id") or created(idle) != idle_at
            or idle_at <= origin_at or idle.get("outcome") != outcome
            or any(m.get("type") in ("synthetic", "compaction", "shell", "skill")
                   for m in messages)):
        return blocked("conflicting-window")
    latest = client.get_session(session_id)
    if (not latest or latest.get("id") != session_id
            or latest.get("outcome") != outcome
            or (latest.get("time") or {}).get("idle") != idle_at
            or int((latest.get("time") or {}).get("busy") or 0) > idle_at
            or session_id in client.active_sessions()
            or client.inbox(session_id)):
        return blocked("latest-state-changed")
    return {"finished": True, "reason": "completed-user-turn-verified",
            "message_id": message_id, "origin_at": origin_at,
            "idle_id": idle.get("id"), "idle_at": idle_at,
            "outcome": outcome, "hold_origin_at": hold_origin,
            "coverage": coverage}

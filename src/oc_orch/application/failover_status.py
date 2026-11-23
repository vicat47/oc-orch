"""只读的 provider 故障切换状态投影（无 Client、无网络、无游标推进）。"""

from __future__ import annotations


def _episode_view(rec: dict) -> dict:
    metadata = rec.get("metadata") or {}
    target = metadata.get("target") or {}
    target_route = None
    if isinstance(target, dict) and target.get("providerID") and target.get("id"):
        from ..domain.failover import route_key

        target_route = route_key(target["providerID"], target["id"],
                                 target.get("variant"))
    return {key: rec.get(key) for key in (
        "episode_id", "project", "task_id", "session_id", "route",
        "category", "status", "revision", "switches_used", "attempts_used",
        "created_at", "updated_at")} | {
            "target_route": target_route,
            "old_attempt_id": rec.get("old_attempt_id"),
            "successor_attempt_id": metadata.get("successor_attempt_id"),
        }


def _operation_view(rec: dict) -> dict:
    metadata = rec.get("metadata") or {}
    return {key: rec.get(key) for key in (
        "operation_id", "episode_id", "session_id", "status", "revision",
        "possible_submitted_at", "created_at", "updated_at")} | {
            "kind": rec.get("kind"),
            "successor_attempt_id": metadata.get("successor_attempt_id"),
            "successor_message_id": metadata.get("successor_message_id"),
        }


def failover_snapshot(failover, outbox, *, task_id: str | None = None,
                      project: str | None = None) -> dict:
    """从只读端口投影故障状态；不返回邮件正文、错误正文或密钥。"""
    guard = failover.guard_valid()
    try:
        episodes = failover.list_records("episode", limit=200)
        relevant = [rec for rec in episodes["records"]
                    if (task_id is None or rec.get("task_id") == task_id)
                    and (project is None or rec.get("project") == project)]
        episode_ids = {rec.get("episode_id") for rec in relevant}
        sessions = {rec.get("session_id") for rec in relevant if rec.get("session_id")}
        routes = {rec.get("route") for rec in relevant if rec.get("route")}
        operations = failover.list_records("operation", limit=200)
        operation_rows = [rec for rec in operations["records"]
                          if rec.get("episode_id") in episode_ids]
        blocks = [rec for rec in failover.list_route_blocks(limit=200)
                  if rec.get("route") in routes]
        locks = [item for item in failover.list_session_locks(limit=200)
                 if item.get("session_id") in sessions]
        mail = [item for item in outbox.list_all()
                if item.get("source_kind") == "failover-episode"
                and item.get("source_id") in episode_ids]
        mail_views = [{key: item.get(key) for key in (
            "item_id", "source_id", "kind", "status", "condition_version",
            "attempts", "max_attempts", "possible_submitted_at", "created_at",
            "updated_at")} for item in mail]
        return {
            "ok": True,
            "read_only": True,
            "network_calls": 0,
            "guard": guard,
            "episodes": [_episode_view(rec) for rec in relevant],
            "operations": [_operation_view(rec) for rec in operation_rows],
            "route_blocks": [{key: rec.get(key) for key in (
                "route", "blocked_at", "until_ms", "expired", "blocked",
                "cleared", "revision")} for rec in blocks],
            "session_locks": [{
                "session_id": item.get("session_id"),
                "operation_id": item.get("operation_id"),
                "episode_id": (item.get("episode") or {}).get("episode_id"),
                "status": (item.get("episode") or {}).get("status"),
            } for item in locks],
            "notifications": mail_views,
            "notification_counts": _count_status(mail_views),
            "more": bool(episodes.get("more") or operations.get("more")),
            "invalid_records": int(episodes.get("invalid_records", 0))
            + int(operations.get("invalid_records", 0)),
        }
    except Exception as exc:
        # 只暴露异常类型，避免把错误消息中可能含的路径/正文带入 JSON 或页面。
        return {"ok": False, "read_only": True, "network_calls": 0,
                "guard": guard, "code": "FAILOVER_STATE_UNKNOWN",
                "error_type": type(exc).__name__, "episodes": [],
                "operations": [], "route_blocks": [], "session_locks": [],
                "notifications": [], "notification_counts": {}}


def _count_status(rows: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for item in rows:
        key = str(item.get("status") or "unknown")
        out[key] = out.get(key, 0) + 1
    return out

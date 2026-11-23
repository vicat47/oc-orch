"""可解释、非授权的路线观测投影；缺失/过期/估计绝不变成零开销或缓存命中。"""
import math

FIELDS = ("context_tokens", "cache_hit", "queue_ms", "remaining_ms", "switch_overhead_ms")


def route_context(task, attempt, now):
    raw = (task.get("metadata") or {}).get("route_observations") or {}
    result = {}
    for field in FIELDS:
        item = raw.get(field) if isinstance(raw, dict) else None
        entry = {"state": "unknown", "value": None, "source": None, "observed_at": None, "kind": "unknown"}
        if isinstance(item, dict) and set(item) == {"value", "source", "observed_at", "expires_at", "kind"}:
            value = item["value"]
            valid = (type(value) is bool if field == "cache_hit" else type(value) in (int, float) and math.isfinite(value) and value >= 0)
            if (valid and isinstance(item["source"], str) and item["source"].strip()
                    and item["kind"] in ("measured", "estimate")
                    and type(item["observed_at"]) is int and type(item["expires_at"]) is int
                    and 0 <= item["observed_at"] <= now and item["expires_at"] > item["observed_at"]):
                stale = now >= item["expires_at"]
                entry.update(state="stale" if stale else item["kind"], source=item["source"],
                             observed_at=item["observed_at"], kind=item["kind"], value=None if stale else value)
        result[field] = entry
    return {"observations": result, "session_id": attempt.get("session_id") if attempt else None,
            "model": attempt.get("model") if attempt else None,
            "continuity_reason": "保留现有获授权路线，避免无证据重建上下文；后续每次动作仍按当前版本重新许可",
            "faster_route_proven": False, "authorization_effect": "none",
            "unknown_is_not_zero": True}

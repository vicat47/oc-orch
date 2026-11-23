"""已显式登记用户回合的有限只读补证预算；网络前持久认领。"""

import json

from ..domain.coordinator_control import CoordinatorControlStateError
from .coordinator_control import CoordinatorControlLedger
from .sqlite_db import meta_get, meta_set, transaction


class CoordinatorTurnObservation:
    MAX_CHECKS = 8
    INTERVAL_MS = 30_000
    WINDOW_MS = 3_600_000

    def __init__(self, conn, clock):
        self.conn, self.clock = conn, clock

    def claim(self, session_id: str, *, expected_revision: int, idle_at: int) -> dict:
        """同原输入跨重启不重置；新终态水位允许一次新证据核验。"""
        with transaction(self.conn):
            snap = CoordinatorControlLedger(self.conn, self.clock).get_session(session_id)
            barrier = snap.get("barrier")
            if not barrier or snap["revision"] != expected_revision:
                return {"allowed": False, "reason": "turn-changed"}
            now = self.clock.now_ms()
            key = f"coordinator-turn-observation:{session_id}"
            identity = [barrier["message_id"], barrier["origin_at"], barrier["operation_id"]]
            raw = meta_get(self.conn, key)
            rec = None
            if raw is not None:
                try:
                    rec = json.loads(raw)
                except (TypeError, ValueError) as exc:
                    raise CoordinatorControlStateError("回合补证预算 JSON 损坏") from exc
                if (not isinstance(rec, dict) or type(rec.get("schema")) is not int
                        or rec["schema"] != 1 or not isinstance(rec.get("identity"), list)
                        or len(rec["identity"]) != 3
                        or not all(isinstance(rec["identity"][i], str)
                                   and rec["identity"][i].strip() for i in (0, 2))
                        or type(rec["identity"][1]) is not int or rec["identity"][1] < 0
                        or any(type(rec.get(k)) is not int or rec[k] < 0
                               for k in ("checks", "next_at", "deadline", "idle_watermark"))):
                    raise CoordinatorControlStateError("回合补证预算记录损坏")
                if rec["deadline"] != rec["identity"][1] + self.WINDOW_MS:
                    raise CoordinatorControlStateError("回合补证期限与原输入不符")
            if rec is None or rec["identity"] != identity:
                rec = {"schema": 1, "identity": identity, "checks": 0,
                       "next_at": 0, "deadline": barrier["origin_at"] + self.WINDOW_MS,
                       "idle_watermark": 0}
            new_fact = idle_at > rec["idle_watermark"]
            exhausted = now >= rec["deadline"] or rec["checks"] >= self.MAX_CHECKS
            if exhausted and not new_fact:
                return {"allowed": False, "reason": "observation-exhausted"}
            if not new_fact and now < rec["next_at"]:
                return {"allowed": False, "reason": "observation-wait"}
            rec["checks"] += 1
            rec["next_at"] = now + self.INTERVAL_MS
            rec["idle_watermark"] = max(rec["idle_watermark"], idle_at)
            meta_set(self.conn, key, json.dumps(rec))
            return {"allowed": True, "reason": "new-terminal" if new_fact else "due",
                    "checks": rec["checks"]}

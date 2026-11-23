"""当前策略×独立授权×控制的单次许可；同连接BEGIN IMMEDIATE排序，无网络。

许可一经提交即possible（含到网络的交接窗）；任何重放只对账，不重新发放。
30秒交接有效期不表示unknown自动变absent，也不撤销已飞出的工作。
"""

from __future__ import annotations

import json

from ..domain import delivery_action as rules
from ..domain.delivery_policy import canonical, fingerprint, identity
from ..domain.errors import LedgerError
from .clock import SystemClock
from .delivery_guard import check_delivery_guard
from .delivery_policy import DeliveryPolicyStore
from .sqlite_db import read_snapshot, transaction

PREFIX = "delivery-action:v1:"
INDEX = "delivery-action-task:v1:"
_TERMINAL = {"terminal_succeeded", "terminal_failed", "terminal_interrupted", "result_collected",
             "result_missing", "verification_failed", "accepted", "rejected", "cancelled", "lost"}


def _key(session_id, message_id, operation):
    return PREFIX + canonical([session_id, message_id, operation])


class DeliveryActions:
    def __init__(self, ledger, clock=None):
        self.ledger, self.conn = ledger, ledger.conn
        self.clock = clock or ledger.clock or SystemClock()
        self.policies = DeliveryPolicyStore(self.conn, self.clock)

    def enabled(self) -> bool:
        return check_delivery_guard(self.conn)

    def attempt_required(self, attempt_id) -> bool:
        with read_snapshot(self.conn):
            return self.enabled() and self.policies.task_snapshot(
                self.ledger.get_attempt(attempt_id)["task_id"])["managed"]

    def input_required(self, input_id) -> bool:
        with read_snapshot(self.conn):
            if not self.enabled():
                return False
            row = self.ledger.get_input(input_id)
            if row is None:
                raise LedgerError("DELIVERY_INPUT_CHANGED", "资源动作输入不存在")
            if row["source"] != "automatic_wake":
                return True  # 受管外部输入需显式授权，untracked不能作为绕过开关
            from .resource_authorization import ResourceAuthorizationStore

            if ResourceAuthorizationStore(self.conn, self.clock).snapshot("coordinator", row["session_id"])["revision"]:
                return True
            for nid in row["notification_ids"]:
                notice = self.ledger.get_notification(nid)
                if notice.get("task_id"):
                    if self.policies.task_snapshot(notice["task_id"])["managed"]:
                        return True
                elif notice.get("project") and self.policies.project_snapshot(notice["project"])["managed"]:
                    return True
            return False

    def _load(self, key):
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row is None:
            return None
        try:
            value = json.loads(row[0])
            digest = value.pop("fingerprint")
            if digest != fingerprint(value) or value["schema"] != 1 or type(value["schema"]) is not int:
                raise ValueError("permit digest/schema")
            if key != _key(value["session_id"], value["message_id"], value["operation"]):
                raise ValueError("permit identity")
            if (type(value["issued_at"]) is not int or type(value["expires_at"]) is not int
                    or not 0 < value["expires_at"] - value["issued_at"] <= rules.HANDOFF_MS):
                raise ValueError("permit time")
            return {**value, "fingerprint": digest}
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise LedgerError("DELIVERY_ACTION_CORRUPT", "持久单次许可损坏；拒绝重发") from exc

    def _check_target_gate(self, session_id):
        from .target_gate import TargetGate
        from ..domain.managed_inputs import gateway_allows

        try:
            blocked = gateway_allows(TargetGate(self.conn, self.clock).get(session_id))
        except Exception as exc:
            raise LedgerError("DELIVERY_TARGET_UNKNOWN", "目标门状态未知；拒绝资源许可") from exc
        if blocked is not None:
            raise LedgerError("DELIVERY_TARGET_BLOCKED", "目标仍受人工/provider门控制", {"reason": blocked})

    def _provider(self, model):
        raw = self.conn.execute("SELECT value FROM meta WHERE key='provider_state'").fetchone()
        try:
            states = json.loads(raw[0]) if raw else {}
            if not isinstance(states, dict):
                raise ValueError("provider state")
            state = states.get(model)
            if state is not None and not isinstance(state, dict):
                raise ValueError("provider entry")
        except (TypeError, ValueError) as exc:
            raise LedgerError("DELIVERY_PROVIDER_BLOCKED", "provider状态损坏，不按空状态放行") from exc
        if state is not None:
            until = state.get("until")
            if until is not None and (type(until) is not int or until > self.clock.now_ms()):
                raise LedgerError("DELIVERY_PROVIDER_BLOCKED", "provider冷却/未知状态不因策略切换解除")

    def _authorization(self, policy, model, *, coordinator=None):
        from .resource_authorization import ResourceAuthorizationStore

        store = ResourceAuthorizationStore(self.conn, self.clock)
        if coordinator is not None:
            # 共用协调者按自身显式scope授权，不读取任何Task的费用预设来改绑父模型。
            return store.authorize("coordinator", coordinator, model=model, preset="elastic")
        task_id, project = policy["scope_id"], policy["project"]
        scope, scope_id = ("task", task_id) if store.snapshot("task", task_id)["revision"] else ("project", project)
        return store.authorize(scope, scope_id, model=model, preset=policy["version"]["policy"]["preset"])

    def _limits(self, policy, attempt, work):
        limits = policy["version"]["policy"]["limits"]
        live = self.conn.execute(
            "SELECT id,status FROM attempts WHERE task_id=? AND site_released_at IS NULL",
            (policy["scope_id"],)).fetchall()
        active = {row["id"] for row in live if row["status"] not in _TERMINAL}
        if len(active | {attempt["id"]}) > limits["max_parallel"]:
            raise LedgerError("DELIVERY_PARALLEL_LIMIT", "分工并发超出当前策略上限")
        if len(active - {attempt["id"]}) and work["reason"] == "single-route-continuity":
            raise LedgerError("DELIVERY_COLLABORATION_REASON_REQUIRED", "新增并行工作须声明关键路径收益")
        if work["kind"] == "challenge" and work["round"] > limits["max_challenge_rounds"]:
            raise LedgerError("DELIVERY_CHALLENGE_LIMIT", "可选挑战轮次超过当前策略；必要检查仍须执行")
        if work["kind"] == "candidate":
            candidates = set()
            prefix = INDEX + canonical([policy["scope_id"]])[:-1] + ","
            for row in self.conn.execute("SELECT value FROM meta WHERE key>=? AND key<?",
                                          (prefix, prefix + "\uffff")):
                permit = self._load(json.loads(row[0])["key"])
                if permit is None:
                    raise LedgerError("DELIVERY_ACTION_CORRUPT", "候选许可索引引用缺失")
                if (permit["policy_revision"] == policy["revision"]
                        and permit["collaboration"]["kind"] == "candidate"):
                    candidates.add(permit["attempt_id"])
            if len(candidates | {attempt["id"]}) > limits["max_candidates"]:
                raise LedgerError("DELIVERY_CANDIDATE_LIMIT", "完整候选超过当前策略独立上限")

    def _issue(self, *, session_id, message_id, operation, input_id, attempt_id,
               task_id, policy, model, authorization, work, body_hash):
        for name, value in (("session_id", session_id), ("message_id", message_id)):
            identity(value, name)
        key = _key(session_id, message_id, operation)
        old = self._load(key)
        if old is not None:
            raise LedgerError("DELIVERY_ACTION_ALREADY_POSSIBLE", "原身份已经获得过单次许可；仅允许对账，不重发",
                              {"session_id": session_id, "message_id": message_id,
                               "input_id": old["input_id"], "policy_revision": old["policy_revision"]})
        now = self.clock.now_ms()
        expires_at = min(now + rules.HANDOFF_MS, authorization["expires_at"])
        if expires_at <= now:
            raise LedgerError("DELIVERY_ACTION_EXPIRED", "资源授权已过期")
        record = {"schema": 1, "session_id": session_id, "message_id": message_id,
                  "operation": operation, "input_id": input_id, "attempt_id": attempt_id,
                  "task_id": task_id, "project": policy["project"] if policy else None,
                  "policy_revision": policy["revision"] if policy else None,
                  "policy_version": policy["version"] if policy else None,
                  "model": model, "authorization": authorization,
                  "collaboration": work, "issued_at": now, "expires_at": expires_at,
                  "state": "possible", "body_hash": body_hash,
                  "route_reason": "current-authorized-route; no measured speedup claimed",
                  "internal_calls_intercepted": False,
                  "coordinator_cost_independent": policy is None}
        record["fingerprint"] = fingerprint(record)
        self.conn.execute("INSERT INTO meta(key,value) VALUES(?,?)", (key, canonical(record)))
        if task_id:
            index = INDEX + canonical([task_id, session_id, message_id, operation])
            self.conn.execute("INSERT INTO meta(key,value) VALUES(?,?)", (index, canonical({"key": key})))
        self.ledger._audit(self.conn, "delivery-policy", "delivery-action.possible", "input",
                           input_id or message_id, None, "possible",
                           {"key": key, "task_id": task_id, "attempt_id": attempt_id,
                            "policy_revision": record["policy_revision"], "model": model,
                            "authorization_revision": authorization["authorization_revision"],
                            "expires_at": expires_at})
        return record

    def attempt(self, attempt_id: str, *, observation: dict) -> dict | None:
        with transaction(self.conn):
            if not self.enabled():
                return None
            att = self.ledger.get_attempt(attempt_id)
            self.ledger.check_attempt_dispatch(attempt_id)
            policy = self.policies.task_snapshot(att["task_id"])
            if not policy["managed"]:
                return None  # 历史兼容只明确未纳管，不补造既往费用授权
            if att["status"] == "dispatch_unknown" or att["dispatch_state"] == "unknown":
                raise LedgerError("DELIVERY_ACTION_ALREADY_POSSIBLE", "发送unknown仅围绕原ID观察；不补发或补造历史许可",
                                  {"attempt_id": attempt_id, "session_id": att["session_id"],
                                   "message_id": att["dispatch_msg_id"]})
            sid, model = att["session_id"], observation.get("model")
            if observation.get("session_id") != sid or model != att["model"]:
                raise LedgerError("DELIVERY_MODEL_CHANGED", "实际session模型与已声明路线不符；不擅自改绑")
            plan = att["metadata"].get("delivery_plan")
            if plan is not None and (not isinstance(plan, dict)
                    or plan.get("scope_id") != att["task_id"] or plan.get("model") != model
                    or plan.get("session_id") != sid or plan.get("message_id") != att["dispatch_msg_id"]
                    or plan.get("body_hash") != fingerprint((att["metadata"].get("dispatch") or {}).get("prompt"))):
                raise LedgerError("DELIVERY_ACTION_CORRUPT", "派单冻结的正文/输入身份已改变；拒绝换体或换ID重投")
            self._provider(model)
            self._check_target_gate(sid)
            work = rules.collaboration(att["metadata"].get("delivery_collaboration"))
            self._limits(policy, att, work)
            authorization = self._authorization(policy, model)
            return self._issue(session_id=sid, message_id=att["dispatch_msg_id"], operation="prompt",
                input_id=None, attempt_id=attempt_id, task_id=att["task_id"], policy=policy,
                model=model, authorization=authorization, work=work,
                body_hash=fingerprint(att["metadata"]["dispatch"]["prompt"]))

    def input_in_tx(self, input_id: str, *, observation: dict, operation: str,
                    owner: str, expected_revision: int) -> dict | None:
        """调用者持同连接写锁；自动wake可与provider探测/possible标记原子组合。"""
        if not self.conn.in_transaction:
            raise RuntimeError("输入资源许可须在本连接写事务内")
        if not self.enabled():
            return None
        if not self.input_required(input_id):
            return None
        row = self.ledger.get_input(input_id)
        if (row is None or row["session_id"] != observation.get("session_id")
                or row.get("revision") != expected_revision):
            raise LedgerError("DELIVERY_INPUT_CHANGED", "输入身份/revision已变化")
        sid, model = row["session_id"], observation.get("model")
        identity(model, "model")
        self._provider(model)
        meta = row.get("metadata") or {}
        tracking = (meta.get("managed_input") or {}).get("tracking") or {}
        policy, attempt = None, None
        task_id = tracking.get("task_id")
        if row["source"] == "automatic_wake":
            notices = [self.ledger.get_notification(nid) for nid in row["notification_ids"]]
            self.ledger.check_automatic_wake(sid, notices)
            from .resource_authorization import ResourceAuthorizationStore

            managed_notices = any(
                self.policies.task_snapshot(n["task_id"])["managed"] if n.get("task_id") else
                self.policies.project_snapshot(n["project"])["managed"] if n.get("project") else False
                for n in notices)
            own_authorization = ResourceAuthorizationStore(self.conn, self.clock).snapshot("coordinator", sid)
            if not managed_notices and not own_authorization["revision"]:
                return None  # 历史兼容范围，不从其他Task的切换扩大费用约束
            # provider半开由WakeAdmission原子占用，本处不把合法半开误作普通blocked。
            authorization = self._authorization(None, model, coordinator=sid)
            work = rules.collaboration(None)
            task_id = None  # 共用wake不是某一Task的父模型策略
        else:
            self._check_target_gate(sid)
            managed = meta.get("managed_input") or {}
            claim = (managed.get("operations") or {}).get("send" if operation == "prompt" else "flush")
            if (row.get("owner") != owner or not isinstance(claim, dict)
                    or claim.get("state") != "claimed"):
                raise LedgerError("DELIVERY_INPUT_CHANGED", "输入发送/flush claim不属于当前调用")
            if task_id:
                policy = self.policies.task_snapshot(task_id)
                attempt = self.ledger.get_attempt(tracking.get("attempt_id"))
                task = self.ledger.get_task(task_id)
                if (task["status"] in ("paused", "cancelled", "accepted") or task.get("closed_at") is not None
                        or task["current_attempt_id"] != attempt["id"]
                        or attempt["session_id"] != sid or attempt["task_id"] != task_id
                        or attempt["generation"] != tracking.get("generation")
                        or attempt.get("stop_requested_at") is not None):
                    raise LedgerError("DELIVERY_INPUT_CHANGED", "任务current/代次/人工控制不允许新输入")
                self.ledger.check_project_dispatch(task["project"])
                if not policy["managed"]:
                    return None
                if attempt["model"] != model:
                    raise LedgerError("DELIVERY_MODEL_CHANGED", "worker实际模型与声明不符")
                if (tracking.get("work_kind") == "write"
                        and (attempt.get("site_released_at") is not None or attempt["status"] in _TERMINAL)):
                    raise LedgerError("DELIVERY_INPUT_CHANGED", "已封存/释放的写输入不能取得许可")
                work = rules.collaboration(managed.get("delivery_collaboration"))
                self._limits(policy, attempt, work)
                authorization = self._authorization(policy, model)
            else:
                # 显式untracked也不能借纯通信绕过已纳管worker的资源守卫。
                for bound in self.conn.execute("SELECT task_id FROM attempts WHERE session_id=?", (sid,)):
                    if self.policies.task_snapshot(bound["task_id"])["managed"]:
                        raise LedgerError("DELIVERY_TRACKING_REQUIRED", "目标包含已纳管任务，须显式追踪，不可untracked绕过")
                self.ledger.check_automatic_wake(sid, [])  # 独立回合hold/用户屏障仍有效
                projects = {r[0] for r in self.conn.execute(
                    "SELECT project FROM tasks WHERE coordinator_session=? UNION "
                    "SELECT project FROM coordinator_leases WHERE holder_session=?", (sid, sid))}
                if row.get("project"):
                    projects.add(row["project"])
                for project in projects:
                    self.ledger.check_project_dispatch(project)
                authorization = self._authorization(None, model, coordinator=sid)
                work = rules.collaboration(None)
        return self._issue(session_id=sid, message_id=row["message_id"], operation=operation,
            input_id=input_id, attempt_id=attempt["id"] if attempt else None,
            task_id=task_id, policy=policy, model=model, authorization=authorization,
            work=work, body_hash=fingerprint(row.get("body")))

    def input(self, input_id, **intent):
        with transaction(self.conn):
            return self.input_in_tx(input_id, **intent)

    def task_inflight(self, task_id: str, *, limit: int = 200) -> dict:
        with read_snapshot(self.conn):
            prefix = INDEX + canonical([task_id])[:-1] + ","
            live, total = [], 0
            current = self.policies.task_snapshot(task_id)
            for row in self.conn.execute("SELECT value FROM meta WHERE key>=? AND key<? ORDER BY key",
                                          (prefix, prefix + "\uffff")):
                try:
                    value = self._load(json.loads(row[0])["key"])
                    if value is None or value["task_id"] != task_id:
                        raise ValueError("permit reference")
                except (ValueError, KeyError, TypeError) as exc:
                    raise LedgerError("DELIVERY_ACTION_CORRUPT", "策略输入索引损坏") from exc
                att = self.ledger.get_attempt(value["attempt_id"]) if value["attempt_id"] else None
                # 没有足够执行归属证据时宁可仍显示possible，不因交接过期假报停止。
                if (not value["input_id"] and att and att.get("terminal_at") is not None
                        and att["terminal_at"] >= value["issued_at"]):
                    continue
                if value["input_id"]:
                    from ..domain.managed_inputs import receipt_view

                    original_input = self.ledger.get_input(value["input_id"])
                    if original_input is None:
                        raise LedgerError("DELIVERY_ACTION_CORRUPT", "在途许可原输入缺失")
                    if receipt_view(original_input, now=self.clock.now_ms()).get("execution") in (
                            "succeeded", "failed", "interrupted"):
                        continue
                total += 1
                if len(live) < limit:
                    live.append({"session_id": value["session_id"], "message_id": value["message_id"],
                        "input_id": value["input_id"], "attempt_id": value["attempt_id"],
                        "policy_revision": value["policy_revision"], "operation": value["operation"],
                        "model": value["model"], "cost": value["authorization"]["resource"]["cost"],
                        "issued_at": value["issued_at"], "expires_at": value["expires_at"],
                        "state": "possible-or-running", "older_policy": value["policy_revision"] != current["revision"]})
            return {"items": live, "total": total, "truncated": total > len(live),
                    "unknown_is_not_stopped": True}

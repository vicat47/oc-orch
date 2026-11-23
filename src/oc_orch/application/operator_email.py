"""独立邮件出口用例：OperatorEmailService（operator-email-alerts 场景 M01–M12）。

编排 domain 纯规则 + outbox 持久化 + MailPort 传输，三本账分离：

- 事件/责任：结构化 incident（来自台账的可行动摘要源，脱敏）；
- 发送意图：outbox 行（事件身份、冻结内容/配置指纹、claim/revision、可能提交、
  期限/预算）；网络副作用始终在事务外；
- 业务处理：本服务**绝不**修改 notifications ACK / Task / 验收 / 写权
  （M11/M12：邮件失败不退业务待办，不因邮件状态代写业务结论）。

接收性拒绝规则（供父/后续审查对账）：

- 回执查询预算在**网络前**经 ``claim_receipt`` 事务原子消耗（owner/revision/
  checks 一步持久，跨重启不重置），随后 ``record_receipt`` 以
  ``expected_receipt_revision`` CAS 提交；缺失 from/message_id/created_at、
  时间窗不符或配置指纹变化都不能 ``verified-accepted``（M10）。
- 收件/发件绑定以 ``identity_fingerprint`` 冻结在行内：地址或发件身份或
  ``sender_ref`` 变化 → 拒发/拒证（auth-blocked），旧 unknown 不能用新配置证明
  （M02/M04/M12）。
- ``dispatch_once`` 只把 ``queued is True`` 当受理；任意其他返回/None/意外异常
  一律 submit-unknown（不假受理、不阻塞独立执行器）（M10/M03）。
- 限频 retry-after 不压短服务要求：只取下限并封顶到有效时钟边界（M11）。

调用方（父进程接线）应在独立执行器/sidecar 中调用 ``dispatch_once`` /
``verify_receipt``，失败不阻塞核心调谐循环（M03）。默认关闭：策略缺范围时
只登记可见 ``pending-auth``，不发信、不登录、不自动安装（M01/M04）。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol

from ..domain.operator_email import (
    MAX_CLOCK_MS,
    POLICY_ENABLED,
    STATUS_ACCEPTED,
    STATUS_AUTH_BLOCKED,
    STATUS_CHANNEL_BLOCKED,
    STATUS_CLAIMED,
    STATUS_PENDING,
    STATUS_PENDING_AUTH,
    STATUS_PERMANENT_FAILED,
    STATUS_RATE_LIMITED,
    STATUS_RETRY_WAIT,
    STATUS_SUBMIT_UNKNOWN,
    EmailPolicy,
    IncidentFact,
    authorize,
    backoff_ms,
    decide_event,
    digest,
    identity_fingerprint,
    policy_state,
    query_authorized,
    stable_base_key,
    stable_event_key,
    validate_fact,
)
from .mail_port import (
    MailAuthError,
    MailChannelError,
    MailError,
    MailPermanentError,
    MailRateLimitError,
    MailSearchUnavailableError,
    MailTransientNotSubmittedError,
    MailTransport,
    MailUnknownError,
)
from .ports import Clock

RECEIPT_WINDOW_SLOP_MS = 60_000
RECEIPT_SEARCH_LIMIT = 50


class MailOutbox(Protocol):
    """持久 outbox 窄端口（application 依赖，不依赖 SQLite 具体实现）。"""

    def upsert_item(self, **kwargs) -> dict: ...
    def get_item(self, item_id: str) -> dict | None: ...
    def items_for_base(self, base_key: str) -> list[dict]: ...
    def list_due(self, *, now: int, limit: int = 10) -> list[dict]: ...
    def list_all(self) -> list[dict]: ...
    def claim(self, item_id: str, *, owner: str, now: int) -> dict: ...
    def mark_result(self, item_id: str, *, expected_revision: int,
                    change: dict, now: int) -> dict: ...
    def update_observation(self, item_id: str, *, observed_at: int,
                           summary: dict, now: int) -> dict: ...
    def auth_block(self, item_id: str, *, reason: str, now: int) -> dict: ...
    def expire_unsubmitted(self, *, base_key: str, now: int,
                           reason: str = "recovered-before-possible-submit") -> int: ...
    def stale_claimed(self, *, now: int, stale_after_ms: int) -> list[dict]: ...
    def claim_receipt(self, item_id: str, *, owner: str, now: int) -> dict: ...
    def record_receipt(self, item_id: str, *, expected_receipt_revision: int,
                       receipt_match: str, external_ref: str | None,
                       now: int) -> dict: ...


class OperatorEmailService:
    """默认关闭、独立执行的邮件提醒出口。"""

    def __init__(self, outbox: MailOutbox, transport: MailTransport,
                 clock: Clock, policy: EmailPolicy):
        self._outbox = outbox
        self._transport = transport
        self._clock = clock
        self._policy = policy

    # ---- 只读视图（含策略状态，不含地址/凭据） -------------------------
    def policy_status(self) -> dict:
        p = self._policy
        return {
            "state": policy_state(p),
            "enabled": bool(p.enabled),
            "unattended": bool(p.unattended),
            "sender_ref": p.sender.ref if p.sender else None,
            "projects": sorted(p.projects),
            "event_kinds": sorted(p.event_kinds),
            "recipient_refs": [r.ref for r in p.recipients],
            "retry_min_ms": p.retry_min_ms,
            "retry_max_ms": p.retry_max_ms,
            "max_attempts": p.max_attempts,
        }

    def list_items(self) -> list[dict]:
        return self._outbox.list_all()

    def get_item(self, item_id: str) -> dict | None:
        return self._outbox.get_item(item_id)

    # ---- 配置绑定（冻结指纹；不存敏感地址原文） ------------------------
    def _bindings(self, item: dict):
        """从行内 ref + 当前策略解析绑定；(recipient, sender) 或 None。"""
        if policy_state(self._policy) != POLICY_ENABLED:
            return None
        ref = item.get("recipient_ref")
        recipient = self._policy.recipient(ref) if ref else None
        if recipient is None or self._policy.sender is None:
            return None
        if self._policy.sender.ref != (item.get("sender_ref") or ""):
            return None
        r_fp = identity_fingerprint("recipient", recipient.ref, recipient.address)
        s_fp = identity_fingerprint("sender", self._policy.sender.ref,
                                    self._policy.sender.identity)
        if r_fp != item.get("recipient_fingerprint") \
                or s_fp != item.get("sender_fingerprint"):
            return None
        return ({"address": recipient.address},
                {"ref": self._policy.sender.ref,
                 "identity": self._policy.sender.identity})

    # ---- 事件登记（M01/M02/M05/M06/M07/M12） -----------------------------
    def enqueue_incident(self, fact: IncidentFact | dict, *,
                         source_binding: dict | None = None) -> dict:
        """登记一次 incident 观察；只在此建立/去重事件身份，网络在 dispatch。

        ``source_binding``（可选）：纯本地源绑定，由 EmailOutbox 在建立/升级
        行的同一事务内重验（入 outbox 前重验原 source 归属/条件版本/人工控制；
        见 adapters.email_outbox）。重验失败 → ``source-recheck-failed``（不建行）。
        """
        if not isinstance(fact, IncidentFact):
            try:
                fact = IncidentFact.from_dict(fact)
            except (TypeError, ValueError) as exc:
                return {"result": "invalid-incident", "reason": str(exc)}
        valid, reason = validate_fact(fact)
        if not valid:
            return {"result": "invalid-incident", "reason": reason}

        now = int(self._clock.now_ms())
        base_key = stable_base_key(fact.project, fact.incident_type,
                                   fact.incident_id, fact.recipient_ref)
        rows = self._outbox.items_for_base(base_key)
        decision = decide_event(fact, rows=rows)

        if decision.get("expire_unsubmitted"):
            # M12：恢复发生在未可能提交之前 → 抑制首封/提醒；不发孤立恢复。
            expired = self._outbox.expire_unsubmitted(base_key=base_key, now=now)
            return {"result": "suppressed",
                    "reason": decision["reason"], "expired": expired,
                    "base_key": base_key}

        event_kind = decision.get("event")
        if event_kind is None:
            # 重复观察 → 只更新同一 incident 摘要（M06）；正常暂停/未知不催。
            if decision["reason"] == "duplicate-observation" and rows:
                promoted = self._promote_pending_auth(rows, base_key, fact, now)
                if promoted is not None:
                    return promoted
                latest = max(rows, key=lambda r: (r.get("created_at") or 0,
                                                  r.get("item_id") or ""))
                self._outbox.update_observation(
                    latest["item_id"],
                    observed_at=fact.observed_at,
                    summary=_observation_summary(fact), now=now)
            return {"result": "no-new-event", "reason": decision["reason"],
                    "base_key": base_key}

        event_key = stable_event_key(fact.project, fact.incident_type,
                                     fact.incident_id, event_kind,
                                     fact.condition_version, fact.recipient_ref)
        auth = authorize(self._policy, project=fact.project, kind=event_kind,
                         recipient_ref=fact.recipient_ref)
        if not auth["allowed"]:
            # M01/M04：未授权/越界 → 可见待授权记录，不发送。
            content = digest(fact, event_id=event_key, kind=event_kind)
            out = self._outbox.upsert_item(
                item_id=event_key, base_key=base_key, event_key=event_key,
                project=fact.project, incident_id=fact.incident_id,
                incident_type=fact.incident_type, kind=event_kind,
                condition_version=fact.condition_version,
                escalation_level=fact.escalation_level,
                recipient_ref=fact.recipient_ref,
                sender_ref=_sender_ref(self._policy),
                recipient_fingerprint=_fingerprint_of(self._policy, "recipient",
                                                      fact.recipient_ref),
                sender_fingerprint=_sender_fingerprint(self._policy),
                subject=content["subject"], body=content["body"],
                content_hash=content["content_hash"],
                status=STATUS_PENDING_AUTH,
                first_seen_at=fact.first_seen_at or fact.observed_at,
                observed_at=fact.observed_at, summary=_observation_summary(fact),
                max_attempts=self._policy.max_attempts,
                receipt_limit=self._policy.receipt_check_limit,
                deadline_at=None, now=now, source_binding=source_binding)
            if out.get("reason") == "source-recheck-failed":
                return {"result": "source-recheck-failed",
                        "reason": out.get("recheck_reason") or "source-stale",
                        "item_id": event_key, "base_key": base_key}
            return {"result": "pending-auth", "reason": auth["reason"],
                    "item_id": event_key, "created": out.get("created"),
                    "base_key": base_key}

        # 授权范围内：pending 待发。已存在同身份行只刷新观察（去重），
        # pending-auth 行在授权到位后自动升级（不重复建身份）并冻结新绑定。
        content = digest(fact, event_id=event_key, kind=event_kind)
        out = self._outbox.upsert_item(
            item_id=event_key, base_key=base_key, event_key=event_key,
            project=fact.project, incident_id=fact.incident_id,
            incident_type=fact.incident_type, kind=event_kind,
            condition_version=fact.condition_version,
            escalation_level=fact.escalation_level,
            recipient_ref=fact.recipient_ref,
            sender_ref=_sender_ref(self._policy),
            recipient_fingerprint=_fingerprint_of(self._policy, "recipient",
                                                  fact.recipient_ref),
            sender_fingerprint=_sender_fingerprint(self._policy),
            subject=content["subject"], body=content["body"],
            content_hash=content["content_hash"],
            status=STATUS_PENDING,
            first_seen_at=fact.first_seen_at or fact.observed_at,
            observed_at=fact.observed_at, summary=_observation_summary(fact),
            max_attempts=self._policy.max_attempts,
            receipt_limit=self._policy.receipt_check_limit,
            deadline_at=None, now=now, source_binding=source_binding)
        if out.get("reason") == "source-recheck-failed":
            return {"result": "source-recheck-failed",
                    "reason": out.get("recheck_reason") or "source-stale",
                    "item_id": event_key, "base_key": base_key}
        if out["created"]:
            return {"result": "enqueued", "kind": event_kind,
                    "reason": decision["reason"], "item_id": event_key,
                    "base_key": base_key}
        if (out.get("item") or {}).get("status") == STATUS_PENDING:
            return {"result": "enqueued", "kind": event_kind,
                    "reason": "pending-auth-promoted", "promoted": True,
                    "item_id": event_key, "base_key": base_key}
        return {"result": "no-new-event", "reason": "duplicate-observation",
                "item_id": event_key, "base_key": base_key}

    # ---- 发送（网络在事务外；M03/M09/M10/M11/M12） ----------------------
    def dispatch_once(self, *, owner: str,
                      item_id: str | None = None) -> dict:
        """独立执行器/sidecar 调用：认领一个到期行并发送。

        顺序：到期候选 → 复核授权与**冻结绑定** → 事务认领（持久
        claim/revision/可能提交 + attempt cap）→ 事务外传输 → claim/revision
        回写。任何非 ``queued is True`` 的结果或意外异常 → submit-unknown
        （不假受理、不阻塞独立执行器）；除台账状态损坏外不抛出。
        """
        now = int(self._clock.now_ms())
        if item_id is None:
            due = self._outbox.list_due(now=now, limit=1)
            if not due:
                return {"result": "nothing-due"}
            item_id = due[0]["item_id"]
        item = self._outbox.get_item(item_id)
        if item is None:
            return {"result": "missing", "item_id": item_id}

        state = policy_state(self._policy)
        if state != POLICY_ENABLED:
            self._outbox.auth_block(item_id, reason=state, now=now)
            return {"result": "auth-blocked", "reason": state,
                    "item_id": item_id}

        auth = authorize(self._policy, project=item.get("project"),
                         kind=item.get("kind"),
                         recipient_ref=item.get("recipient_ref"))
        if not auth["allowed"]:
            self._outbox.auth_block(item_id, reason=auth["reason"], now=now)
            return {"result": "auth-blocked", "reason": auth["reason"],
                    "item_id": item_id}

        bindings = self._bindings(item)
        if bindings is None:
            # 配置/绑定与行内冻结不一致（地址/发件身份/sender_ref 变化）→ 拒发。
            reason = self._binding_mismatch_reason(item)
            self._outbox.auth_block(item_id, reason=reason, now=now)
            return {"result": "auth-blocked", "reason": reason,
                    "item_id": item_id}
        recipient, sender = bindings
        frozen_max = item.get("max_attempts") or self._policy.max_attempts

        claim = self._outbox.claim(item_id, owner=owner, now=now)
        if not claim["allowed"]:
            if claim["reason"] == "attempt-budget-exhausted":
                return {"result": "permanent-failed",
                        "reason": "attempt-budget-exhausted",
                        "item_id": item_id}
            return {"result": "not-claimed", "reason": claim["reason"],
                    "item_id": item_id, "item": claim.get("item")}
        revision = claim["revision"]
        claimed_item = claim["item"]
        attempts = int(claimed_item.get("attempts") or 0)

        # ---- 网络副作用在事务外 ------------------------------------------
        try:
            send_result = self._transport.send(
                to=recipient["address"], sender_ref=sender["ref"],
                sender=sender["identity"],
                subject=claimed_item["subject"], body=claimed_item["body"],
                timeout_seconds=self._policy.send_timeout_seconds)
        except MailUnknownError as exc:
            # M10：无法证明受理/未受理 → 保持可能已提交，不重发，等回执补证。
            self._outbox.mark_result(
                item_id, expected_revision=revision,
                change={"status": STATUS_SUBMIT_UNKNOWN,
                        "failure_class": "unknown",
                        "attempts": attempts + 1,
                        "last_error": str(exc)[:200]}, now=now)
            return {"result": "submit-unknown", "item_id": item_id,
                    "reason": str(exc)[:200]}
        except MailRateLimitError as exc:
            # M11：可信限频 → 按服务给出的 retry-after 等待（不压短只封顶）。
            if attempts + 1 >= frozen_max:
                self._outbox.mark_result(
                    item_id, expected_revision=revision,
                    change={"status": STATUS_PERMANENT_FAILED,
                            "failure_class": "rate-limited",
                            "attempts": attempts + 1,
                            "last_error": "rate-limit-budget-exhausted"}, now=now)
                return {"result": "permanent-failed",
                        "reason": "rate-limit-budget-exhausted",
                        "item_id": item_id}
            retry_after = max(1, int(exc.retry_after_ms or 0))
            next_at = min(MAX_CLOCK_MS, now + retry_after)
            self._outbox.mark_result(
                item_id, expected_revision=revision,
                change={"status": STATUS_RATE_LIMITED,
                        "failure_class": "rate-limited",
                        "attempts": attempts + 1,
                        "next_attempt_at": next_at,
                        "retry_after_ms": next_at - now,
                        "possible_submitted_at": None,
                        "last_error": "rate-limited"}, now=now)
            return {"result": "rate-limited", "item_id": item_id,
                    "next_attempt_at": next_at}
        except MailTransientNotSubmittedError as exc:
            # M11：已证未提交瞬态 → 有界退避（预算跨重启持久，不重置）。
            if attempts + 1 >= frozen_max:
                self._outbox.mark_result(
                    item_id, expected_revision=revision,
                    change={"status": STATUS_PERMANENT_FAILED,
                            "failure_class": "transient",
                            "attempts": attempts + 1,
                            "possible_submitted_at": None,
                            "last_error": "transient-budget-exhausted"}, now=now)
                return {"result": "permanent-failed",
                        "reason": "transient-budget-exhausted",
                        "item_id": item_id}
            delay = backoff_ms(attempts + 1,
                               retry_min_ms=self._policy.retry_min_ms,
                               retry_max_ms=self._policy.retry_max_ms)
            next_at = now + delay
            self._outbox.mark_result(
                item_id, expected_revision=revision,
                change={"status": STATUS_RETRY_WAIT,
                        "failure_class": "transient",
                        "attempts": attempts + 1,
                        "possible_submitted_at": None,
                        "next_attempt_at": next_at,
                        "last_error": str(exc)[:200]}, now=now)
            return {"result": "retry-scheduled", "item_id": item_id,
                    "next_attempt_at": next_at}
        except MailAuthError as exc:
            # M04：授权失效/凭据失效 → 可见 auth-blocked，不再自动重试。
            self._outbox.mark_result(
                item_id, expected_revision=revision,
                change={"status": STATUS_AUTH_BLOCKED,
                        "failure_class": "auth",
                        "attempts": attempts + 1,
                        "last_error": str(exc)[:200]}, now=now)
            return {"result": "auth-blocked", "item_id": item_id,
                    "reason": str(exc)[:200]}
        except MailChannelError as exc:
            self._outbox.mark_result(
                item_id, expected_revision=revision,
                change={"status": STATUS_CHANNEL_BLOCKED,
                        "failure_class": "channel",
                        "attempts": attempts + 1,
                        "last_error": str(exc)[:200]}, now=now)
            return {"result": "channel-blocked", "item_id": item_id,
                    "reason": str(exc)[:200]}
        except MailPermanentError as exc:
            self._outbox.mark_result(
                item_id, expected_revision=revision,
                change={"status": STATUS_PERMANENT_FAILED,
                        "failure_class": "permanent",
                        "attempts": attempts + 1,
                        "last_error": str(exc)[:200]}, now=now)
            return {"result": "permanent-failed", "item_id": item_id,
                    "reason": str(exc)[:200]}
        except MailError as exc:
            # 未知分类兜底：保持可能已提交，不重发。
            self._outbox.mark_result(
                item_id, expected_revision=revision,
                change={"status": STATUS_SUBMIT_UNKNOWN,
                        "failure_class": "unknown",
                        "attempts": attempts + 1,
                        "last_error": str(exc)[:200]}, now=now)
            return {"result": "submit-unknown", "item_id": item_id,
                    "reason": str(exc)[:200]}
        except Exception as exc:  # noqa: BLE001 - 独立执行器不应被意外异常打崩
            # 意外运行时异常：不能证明受理 → 保持可能已提交，不阻塞执行器。
            self._outbox.mark_result(
                item_id, expected_revision=revision,
                change={"status": STATUS_SUBMIT_UNKNOWN,
                        "failure_class": "unknown",
                        "attempts": attempts + 1,
                        "last_error": f"unexpected:{type(exc).__name__}"[:200]},
                now=now)
            return {"result": "submit-unknown", "item_id": item_id,
                    "reason": f"unexpected:{type(exc).__name__}"[:200]}
        else:
            # 接收性拒绝：只有 queued is True 才算服务受理（非送达/已读）。
            if not isinstance(send_result, dict) \
                    or send_result.get("queued") is not True:
                self._outbox.mark_result(
                    item_id, expected_revision=revision,
                    change={"status": STATUS_SUBMIT_UNKNOWN,
                            "failure_class": "unknown",
                            "attempts": attempts + 1,
                            "last_error": "queued-not-confirmed"}, now=now)
                return {"result": "submit-unknown", "item_id": item_id,
                        "reason": "queued-not-confirmed"}
            self._outbox.mark_result(
                item_id, expected_revision=revision,
                change={"status": STATUS_ACCEPTED,
                        "submitted_at": now,
                        "attempts": attempts + 1}, now=now)
            return {"result": "accepted", "outcome": "queued",
                    "item_id": item_id}

    # ---- 回执补证（M10/M12：网络前 claim 预算 + CAS；晚到回执只更新原记录）
    def verify_receipt(self, item_id: str | None = None, *, owner: str) -> dict:
        """对 submit-unknown/claimed 行做最小回执查询；唯一匹配才补记受理。

        配置/授权不符时不消耗预算直接拒绝；否则 ``claim_receipt`` 原子消耗
        预算后事务外查询，再以 ``expected_receipt_revision`` CAS 提交结果。
        """
        now = int(self._clock.now_ms())
        if item_id is None:
            rows = self._outbox.list_all()
            candidates = [r for r in rows
                          if r.get("status") in (STATUS_SUBMIT_UNKNOWN,
                                                 STATUS_CLAIMED)
                          and (r.get("receipt_checks") or 0)
                          < (r.get("receipt_limit") or 1)]
            if not candidates:
                # 无可查询但存在已耗尽的未知项 → 显式报告（预算可见，不消耗）
                exhausted = [r for r in rows
                             if r.get("status") in (STATUS_SUBMIT_UNKNOWN,
                                                    STATUS_CLAIMED)
                             and (r.get("receipt_checks") or 0)
                             >= (r.get("receipt_limit") or 1)]
                if exhausted:
                    return {"result": "receipt-check-exhausted",
                            "item_id": None, "count": len(exhausted)}
                return {"result": "nothing-to-verify"}
            candidates.sort(key=lambda r: (r.get("possible_submitted_at") or 0,
                                           r.get("item_id") or ""))
            item_id = candidates[0]["item_id"]
        item = self._outbox.get_item(item_id)
        if item is None:
            return {"result": "missing", "item_id": item_id}
        if item.get("status") not in (STATUS_SUBMIT_UNKNOWN, STATUS_CLAIMED):
            return {"result": "not-applicable", "item_id": item_id,
                    "status": item.get("status")}

        # 最小回执查询授权独立于发送授权；未被显式授权 → 拒绝且不消耗预算。
        if query_authorized(self._policy) is not True:
            return {"result": "queries-not-authorized",
                    "reason": "receipt-queries-not-authorized",
                    "item_id": item_id}

        # 授权/配置指纹复核（不消耗预算）：旧 unknown 不能用新配置证明。
        bindings = self._bindings(item)
        if bindings is None:
            return {"result": "auth-blocked",
                    "reason": self._binding_mismatch_reason(item),
                    "item_id": item_id}
        recipient, sender = bindings

        claim = self._outbox.claim_receipt(item_id, owner=owner, now=now)
        if not claim["allowed"]:
            return {"result": claim["reason"], "item_id": item_id,
                    "item": claim.get("item")}
        receipt_revision = claim["receipt_revision"]

        after = max(0, int(item.get("possible_submitted_at") or 0)
                    - RECEIPT_WINDOW_SLOP_MS)
        try:
            results = self._transport.search_receipt(
                subject=item.get("subject") or "", to=recipient["address"],
                after_ms=after, before_ms=now, limit=RECEIPT_SEARCH_LIMIT,
                timeout_seconds=self._policy.search_timeout_seconds)
        except MailSearchUnavailableError as exc:
            self._outbox.record_receipt(
                item_id, expected_receipt_revision=receipt_revision,
                receipt_match="unavailable", external_ref=None, now=now)
            return {"result": "search-unavailable", "item_id": item_id,
                    "reason": str(exc)[:200]}

        complete, hit_count = _exact_receipt_matches(
            results, subject=item.get("subject") or "",
            to_address=recipient["address"], from_address=sender["identity"],
            after_ms=after, before_ms=now)
        if not hit_count:
            # 无匹配 ≠ 未发送（索引/过滤/时区差异），保持 unknown 不重发。
            applied = self._outbox.record_receipt(
                item_id, expected_receipt_revision=receipt_revision,
                receipt_match="none", external_ref=None, now=now)
            return {"result": "stale-receipt" if not applied["applied"]
                    else "no-match", "item_id": item_id}
        if len(complete) == 1 and hit_count == 1:
            msg_id = complete[0].get("message_id")
            applied = self._outbox.record_receipt(
                item_id, expected_receipt_revision=receipt_revision,
                receipt_match="unique", external_ref=msg_id, now=now)
            if not applied["applied"]:
                return {"result": "stale-receipt", "item_id": item_id,
                        "reason": applied["reason"]}
            return {"result": "verified-accepted", "item_id": item_id,
                    "external_ref": msg_id}
        # 多条完全命中，或存在部分命中（缺 from/ID/created_at/时间窗不符等）
        # → 不能证明归属，保持 unknown。
        applied = self._outbox.record_receipt(
            item_id, expected_receipt_revision=receipt_revision,
            receipt_match="ambiguous", external_ref=None, now=now)
        return {"result": "stale-receipt" if not applied["applied"]
                else "ambiguous", "item_id": item_id,
                "hits": hit_count, "complete": len(complete)}

    # ---- 陈旧认领（M09：崩溃后不盲重发，转未知等回执） ------------------
    def reconcile_stale_claimed(self) -> dict:
        now = int(self._clock.now_ms())
        rows = self._outbox.stale_claimed(
            now=now, stale_after_ms=int(self._policy.stale_claim_after_ms))
        return {"result": "reconciled", "count": len(rows),
                "item_ids": [r.get("item_id") for r in rows]}

    # ---- 内部：配置不匹配原因（供 auth_block/拒绝） ---------------------
    def _binding_mismatch_reason(self, item: dict) -> str:
        if policy_state(self._policy) != POLICY_ENABLED:
            return policy_state(self._policy)
        sender_ref = item.get("sender_ref") or ""
        if self._policy.sender is not None and self._policy.sender.ref != sender_ref:
            return "sender-ref-changed"
        ref = item.get("recipient_ref")
        recipient = self._policy.recipient(ref) if ref else None
        if recipient is None:
            return "recipient-out-of-scope"
        r_fp = identity_fingerprint("recipient", recipient.ref, recipient.address)
        if r_fp != item.get("recipient_fingerprint"):
            return "recipient-config-changed"
        s_fp = identity_fingerprint("sender", self._policy.sender.ref,
                                    self._policy.sender.identity)
        if s_fp != item.get("sender_fingerprint"):
            return "sender-config-changed"
        return "config-changed"

    # ---- 内部：pending-auth 授权到位后升级（M01→M02 过渡） ---------------
    def _promote_pending_auth(self, rows: list[dict], base_key: str,
                              fact: IncidentFact, now: int) -> dict | None:
        pa = next((r for r in rows
                   if r.get("status") == STATUS_PENDING_AUTH), None)
        if pa is None:
            return None
        auth = authorize(self._policy, project=pa.get("project"),
                         kind=pa.get("kind"),
                         recipient_ref=pa.get("recipient_ref"))
        if not auth["allowed"]:
            return None
        out = self._outbox.upsert_item(
            item_id=pa["item_id"], base_key=base_key, event_key=pa["item_id"],
            project=pa.get("project") or fact.project,
            incident_id=pa.get("incident_id") or fact.incident_id,
            incident_type=pa.get("incident_type") or fact.incident_type,
            kind=pa["kind"],
            condition_version=(pa.get("condition_version")
                               or fact.condition_version),
            escalation_level=(pa.get("escalation_level")
                              or fact.escalation_level),
            recipient_ref=pa.get("recipient_ref") or fact.recipient_ref,
            sender_ref=_sender_ref(self._policy),
            recipient_fingerprint=_fingerprint_of(self._policy, "recipient",
                                                  fact.recipient_ref),
            sender_fingerprint=_sender_fingerprint(self._policy),
            subject=pa.get("subject") or "", body=pa.get("body") or "",
            content_hash=pa.get("content_hash") or "",
            status=STATUS_PENDING,
            first_seen_at=(pa.get("first_seen_at")
                           or fact.first_seen_at or fact.observed_at),
            observed_at=fact.observed_at, summary=_observation_summary(fact),
            max_attempts=(pa.get("max_attempts")
                          or self._policy.max_attempts),
            receipt_limit=(pa.get("receipt_limit")
                           or self._policy.receipt_check_limit),
            deadline_at=None, now=now)
        return {"result": "enqueued", "kind": pa.get("kind"),
                "reason": "pending-auth-promoted", "promoted": True,
                "item_id": pa["item_id"], "base_key": base_key,
                "status": (out.get("item") or {}).get("status")}


# ---- 纯工具 ---------------------------------------------------------------
def _fingerprint_of(policy: EmailPolicy, kind: str, ref: str) -> str:
    value = ""
    if kind == "recipient":
        r = policy.recipient(ref) if ref else None
        if r is not None:
            value = r.address
    elif kind == "sender":
        if policy.sender is not None:
            value = policy.sender.identity
    return identity_fingerprint(kind, ref or "", value)


def _sender_fingerprint(policy: EmailPolicy) -> str:
    ref = _sender_ref(policy)
    value = policy.sender.identity if policy.sender is not None else ""
    return identity_fingerprint("sender", ref, value)


def _sender_ref(policy: EmailPolicy) -> str:
    return policy.sender.ref if policy.sender is not None else "unassigned"


def _observation_summary(fact: IncidentFact) -> dict:
    return {
        "condition_key": fact.condition_key[:200],
        "blocked_reason": fact.blocked_reason[:200],
        "impact": fact.impact[:200],
        "decision_needed": fact.decision_needed[:200],
        "escalation_level": fact.escalation_level,
        "recovered": bool(fact.recovered),
    }


def _parse_iso(ts: object) -> int | None:
    """严格 ISO-8601 → epoch 毫秒；不可解析/缺 T 分隔一律 None。"""
    if not isinstance(ts, str) or not ts.strip():
        return None
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    elif s.endswith("z"):
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        return int(dt.timestamp() * 1000)
    except (OverflowError, OSError, ValueError):
        return None


def _exact_receipt_matches(results: list, *, subject: str, to_address: str,
                           from_address: str, after_ms: int,
                           before_ms: int) -> tuple[list, int]:
    """回执唯一匹配：精确 subject + 原冻结收件人 + 原发件人 + 有效 message_id +
    时间范围内的 created_at。返回 ``(complete, hit_count)``。

    只把“完全满足全部条件”的条目计入 complete；hit_count 是 subject+收件人
    命中的候选总数（含缺 from/ID/created_at/时间窗不符的旁系，用于歧义判定）。"""
    complete: list = []
    hit_count = 0
    for r in results if isinstance(results, list) else []:
        if not isinstance(r, dict):
            continue
        if r.get("subject") != subject:
            continue  # 精确主题（含完整事件 ID）
        to_list = r.get("to") or []
        if not isinstance(to_list, list) \
                or not any(isinstance(t, dict) and t.get("email") == to_address
                           for t in to_list):
            continue  # 原冻结收件人
        hit_count += 1
        if not _receipt_record_valid(r, from_address, after_ms, before_ms):
            continue
        complete.append(r)
    return complete, hit_count


def _receipt_record_valid(r: dict, from_address: str, after_ms: int,
                          before_ms: int) -> bool:
    """唯一受理证据所需字段：from 匹配原发件人 + 有效 message_id + 有效时间。"""
    raw_from = r.get("from") or {}
    from_email = raw_from.get("email") if isinstance(raw_from, dict) else None
    if not isinstance(from_email, str) or not from_email \
            or from_email != from_address:
        return False  # 缺/错发件人 → 不能证明是本系统发出
    msg_id = r.get("message_id")
    if not isinstance(msg_id, str) or not msg_id.strip():
        return False  # 缺有效 message_id → 不能 verified-accepted
    created = _parse_iso(r.get("created_at"))
    if created is None or not (after_ms <= created <= before_ms):
        return False  # 缺/坏 created_at 或时间窗外 → 不能 verified-accepted
    return True


__all__ = ["MailOutbox", "OperatorEmailService", "RECEIPT_WINDOW_SLOP_MS"]
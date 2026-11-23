#!/usr/bin/env python3
"""operator-email-alerts 场景 M01–M12：domain 纯规则 + OperatorEmailService 行为。

服务层测试使用真临时 SQLite（EmailOutbox）、FakeClock 与可注入 fake transport，
覆盖：默认关闭/授权矩阵/事件去重/恢复抑制/发送未知/有界重试/回执补证/晚到
回执/崩溃窗口/配置冻结指纹/严格回执字段/意外异常保 unknown；并断言发送绝不
修改业务 notifications/tasks/audit（三本账分离）。

只调用本组 domain/application/adapters，不触 runtime/真实网络/邮箱。
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.adapters.sqlite_db import init  # noqa: E402
from oc_orch.adapters.email_outbox import EmailOutbox  # noqa: E402
from oc_orch.application.mail_port import (  # noqa: E402
    MailAuthError,
    MailRateLimitError,
    MailTransientNotSubmittedError,
    MailUnknownError,
)
from oc_orch.application.operator_email import OperatorEmailService  # noqa: E402
from oc_orch.domain.operator_email import (  # noqa: E402
    CONDITION_HUMAN,
    CONDITION_PAUSE,
    CONDITION_UNKNOWN,
    KIND_ESCALATION,
    KIND_FIRST,
    KIND_RECOVERY,
    KIND_UPDATE,
    POLICY_DISABLED,
    POLICY_ENABLED,
    POLICY_MISSING_SCOPE,
    STATUS_ACCEPTED,
    STATUS_AUTH_BLOCKED,
    STATUS_CLAIMED,
    STATUS_EXPIRED_SUPPRESSED,
    STATUS_PENDING,
    STATUS_PENDING_AUTH,
    STATUS_PERMANENT_FAILED,
    STATUS_RATE_LIMITED,
    STATUS_RETRY_WAIT,
    STATUS_SUBMIT_UNKNOWN,
    EmailPolicy,
    IncidentFact,
    Recipient,
    Sender,
    authorize,
    backoff_ms,
    decide_event,
    digest,
    policy_state,
    redact_fields,
    redact_text,
    stable_base_key,
    stable_event_key,
    validate_fact,
)
from tests.fakes import FakeClock  # noqa: E402


def enabled_policy(**overrides) -> EmailPolicy:
    kwargs = dict(
        enabled=True,
        sender=Sender("oc-ops", "ops@example.test"),
        recipients=(Recipient("op-primary", "operator@example.test"),),
        projects=frozenset({"proj-a"}),
        event_kinds=frozenset({KIND_FIRST, KIND_UPDATE, KIND_ESCALATION,
                               KIND_RECOVERY}),
        unattended=True,
        receipt_queries_authorized=True,
        retry_min_ms=10_000,
        retry_max_ms=80_000,
        max_attempts=3,
        receipt_check_limit=3,
    )
    kwargs.update(overrides)
    return EmailPolicy(**kwargs)


def fact(**kw) -> IncidentFact:
    base = dict(
        incident_id="inc-1", project="proj-a", incident_type="manual-step",
        condition_key="waiting-human", condition_version=1,
        recipient_ref="op-primary", condition_kind=CONDITION_HUMAN,
        blocked_reason="需要人工决策", impact="等待决策", decision_needed="选A或B",
        first_seen_at=1_000_000, observed_at=1_000_000,
    )
    base.update(kw)
    return IncidentFact(**base)


def fact_dict(**kw) -> dict:
    base = dict(
        incident_id="inc-1", project="proj-a", incident_type="manual-step",
        condition_key="waiting-human", condition_version=1,
        recipient_ref="op-primary", condition_kind="human-responsibility",
        blocked_reason="需要人工决策", decision_needed="选A或B",
        first_seen_at=1_000_000, observed_at=1_000_000,
    )
    base.update(kw)
    return base


class FakeTransport:
    """可注入结果/异常的窄传输桩；记录调用，永不接触真实邮箱/网络。"""

    def __init__(self):
        self.sent: list[dict] = []
        self.searches: list[dict] = []
        self.send_result: object = {"queued": True}
        self.search_result: object = []
        self.logged_in: list[str] = []

    def send(self, **kw):
        self.sent.append(dict(kw))
        if isinstance(self.send_result, Exception):
            raise self.send_result
        return self.send_result

    def search_receipt(self, **kw):
        self.searches.append(dict(kw))
        if isinstance(self.search_result, Exception):
            raise self.search_result
        return list(self.search_result)


class MailBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="mail-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.outbox = EmailOutbox(self.conn, self.clock)
        self.transport = FakeTransport()

    def service(self, policy: EmailPolicy = None) -> OperatorEmailService:
        return OperatorEmailService(self.outbox, self.transport,
                                    self.clock, policy or EmailPolicy())

    def business_rows(self, table: str) -> list:
        return self.conn.execute(f"SELECT * FROM {table}").fetchall()

    def item(self, item_id: str) -> dict | None:
        return self.outbox.get_item(item_id)


# ============================================================================
# domain 纯规则
# ============================================================================
class DomainPolicy(unittest.TestCase):
    def test_default_policy_disabled(self):
        self.assertEqual(policy_state(EmailPolicy()), POLICY_DISABLED)
        self.assertFalse(authorize(EmailPolicy())["allowed"])

    def test_enabled_without_full_scope_is_missing_scope(self):
        partial = EmailPolicy(enabled=True)  # 无 sender/recipient/project/kind
        self.assertEqual(policy_state(partial), POLICY_MISSING_SCOPE)
        partial2 = EmailPolicy(enabled=True, sender=Sender("s", "x@y.test"),
                               recipients=(Recipient("r", "a@b.test"),),
                               projects=frozenset({"p"}),
                               event_kinds=frozenset({KIND_FIRST}),
                               unattended=False)  # 无人值守未授权
        self.assertEqual(policy_state(partial2), POLICY_MISSING_SCOPE)

    def test_arbitrary_bool_cannot_bypass_missing_scope(self):
        # enabled=True 但缺无人值守授权 → 仍不可发（不能用任意 bool 绕过范围）。
        p = EmailPolicy(enabled=True, sender=Sender("s", "x@y.test"),
                        recipients=(Recipient("r", "a@b.test"),),
                        projects=frozenset({"p"}),
                        event_kinds=frozenset({KIND_FIRST}))
        self.assertEqual(policy_state(p), POLICY_MISSING_SCOPE)
        self.assertFalse(authorize(p, project="p", kind=KIND_FIRST,
                                   recipient_ref="r")["allowed"])

    def test_history_email_does_not_imply_authorization(self):
        # 从历史个人邮箱拼出的“授权”缺显式无人值守授权 → 仍 missing-scope。
        historical = EmailPolicy(enabled=True,
                                 sender=Sender("me", "historical@example.test"),
                                 recipients=(Recipient("me", "historical@example.test"),),
                                 projects=frozenset({"p"}),
                                 event_kinds=frozenset({KIND_FIRST}))
        self.assertEqual(policy_state(historical), POLICY_MISSING_SCOPE)
        self.assertFalse(authorize(historical, project="p", kind=KIND_FIRST,
                                   recipient_ref="me")["allowed"])

    def test_authorize_explicit_scope_matrix(self):
        p = enabled_policy()
        self.assertTrue(authorize(p, project="proj-a", kind=KIND_FIRST,
                                  recipient_ref="op-primary")["allowed"])
        self.assertEqual(authorize(p, project="proj-a", kind=KIND_RECOVERY,
                                   recipient_ref="op-primary")["reason"],
                         "authorized")
        self.assertEqual(authorize(p, project="proj-b", kind=KIND_FIRST,
                                   recipient_ref="op-primary")["reason"],
                         "project-out-of-scope")
        restricted = enabled_policy(
            event_kinds=frozenset({KIND_FIRST, KIND_UPDATE, KIND_ESCALATION}))
        self.assertEqual(authorize(restricted, project="proj-a",
                                   kind=KIND_RECOVERY,
                                   recipient_ref="op-primary")["reason"],
                         "kind-out-of-scope")
        self.assertEqual(authorize(p, project="proj-a", kind=KIND_FIRST,
                                   recipient_ref="other")["reason"],
                         "recipient-out-of-scope")

    def test_validate_fact_fail_closed(self):
        bad, reason = validate_fact(fact(condition_version=0))
        self.assertFalse(bad)
        self.assertEqual(reason, "invalid-condition-version")
        bad2, reason2 = validate_fact(fact(incident_id=""))
        self.assertFalse(bad2)
        self.assertEqual(reason2, "missing-incident-identity")


class DomainStrictTypes(unittest.TestCase):
    def test_bool_condition_version_rejected(self):
        with self.assertRaises(ValueError):
            IncidentFact.from_dict(fact_dict(condition_version=True))

    def test_bool_escalation_level_rejected(self):
        with self.assertRaises(ValueError):
            IncidentFact.from_dict(fact_dict(escalation_level=False))

    def test_string_false_recovered_rejected(self):
        with self.assertRaises(ValueError):
            IncidentFact.from_dict(fact_dict(recovered="false"))

    def test_non_int_timestamp_rejected(self):
        with self.assertRaises(ValueError):
            IncidentFact.from_dict(fact_dict(observed_at="now"))

    def test_missing_required_fields_rejected(self):
        with self.assertRaises(ValueError):
            IncidentFact.from_dict(fact_dict(condition_version=0))


class DomainIdentity(unittest.TestCase):
    def test_stable_event_key_independent_of_observation_time(self):
        a = stable_event_key("p", "t", "i", KIND_FIRST, 1, "r")
        b = stable_event_key("p", "t", "i", KIND_FIRST, 1, "r")
        self.assertEqual(a, b)
        self.assertNotEqual(
            a, stable_event_key("p", "t", "i", KIND_FIRST, 2, "r"))
        self.assertNotEqual(
            a, stable_event_key("p", "t", "i", KIND_UPDATE, 1, "r"))
        self.assertNotEqual(
            a, stable_event_key("p", "t", "i", KIND_FIRST, 1, "other"))
        self.assertEqual(
            stable_base_key("p", "t", "i", "r"),
            stable_base_key("p", "t", "i", "r"))


class DomainEventSelection(unittest.TestCase):
    def test_first_human_action(self):
        d = decide_event(fact(), rows=[])
        self.assertEqual(d["event"], KIND_FIRST)
        self.assertEqual(d["reason"], "first-human-action")

    def test_duplicate_observation_same_version(self):
        rows = [{"kind": KIND_FIRST, "condition_version": 1,
                 "escalation_level": 0, "status": STATUS_PENDING,
                 "possible_submitted_at": None}]
        d = decide_event(fact(condition_version=1), rows=rows)
        self.assertIsNone(d["event"])
        self.assertEqual(d["reason"], "duplicate-observation")

    def test_condition_change_creates_update(self):
        rows = [{"kind": KIND_FIRST, "condition_version": 1,
                 "escalation_level": 0, "status": STATUS_ACCEPTED,
                 "possible_submitted_at": 1_000_000}]
        d = decide_event(fact(condition_version=2), rows=rows)
        self.assertEqual(d["event"], KIND_UPDATE)
        self.assertEqual(d["reason"], "condition-changed")

    def test_escalation_point(self):
        rows = [{"kind": KIND_FIRST, "condition_version": 1,
                 "escalation_level": 0, "status": STATUS_PENDING,
                 "possible_submitted_at": None}]
        d = decide_event(fact(condition_version=1, escalation_level=2), rows=rows)
        self.assertEqual(d["event"], KIND_ESCALATION)
        self.assertEqual(d["reason"], "escalation-point")

    def test_normal_pause_never_nags(self):
        d = decide_event(fact(condition_kind=CONDITION_PAUSE), rows=[])
        self.assertIsNone(d["event"])
        self.assertEqual(d["reason"], "normal-pause-no-nag")

    def test_unknown_condition_fail_closed(self):
        d = decide_event(fact(condition_kind=CONDITION_UNKNOWN), rows=[])
        self.assertIsNone(d["event"])
        self.assertEqual(d["reason"], "condition-unknown")

    def test_orphan_recovery_suppressed_without_prior_possible(self):
        rows = [{"kind": KIND_FIRST, "condition_version": 1,
                 "escalation_level": 0, "status": STATUS_PENDING,
                 "possible_submitted_at": None}]
        d = decide_event(fact(recovered=True), rows=rows)
        self.assertIsNone(d["event"])
        self.assertTrue(d["expire_unsubmitted"])
        self.assertEqual(d["reason"], "recovered-before-possible-submit")

    def test_recovery_only_with_prior_possible(self):
        rows = [{"kind": KIND_FIRST, "condition_version": 1,
                 "escalation_level": 0, "status": STATUS_ACCEPTED,
                 "possible_submitted_at": 1_000_000}]
        d = decide_event(fact(recovered=True), rows=rows)
        self.assertEqual(d["event"], KIND_RECOVERY)
        self.assertFalse(d["expire_unsubmitted"])

    def test_duplicate_recovery_once(self):
        rows = [{"kind": KIND_FIRST, "condition_version": 1,
                 "escalation_level": 0, "status": STATUS_ACCEPTED,
                 "possible_submitted_at": 1_000_000},
                {"kind": KIND_RECOVERY, "condition_version": 2,
                 "escalation_level": 0, "status": STATUS_ACCEPTED,
                 "possible_submitted_at": 1_100_000}]
        d = decide_event(fact(recovered=True, condition_version=2), rows=rows)
        self.assertIsNone(d["event"])
        self.assertEqual(d["reason"], "duplicate-recovery")


class DomainDigest(unittest.TestCase):
    def test_deterministic_and_minimal(self):
        f = fact(blocked_reason="需要人工决策 token sk-abc1234567890")
        a = digest(f, event_id="ev-1", kind=KIND_FIRST)
        b = digest(f, event_id="ev-1", kind=KIND_FIRST)
        self.assertEqual(a, b)
        self.assertIn("ev-1", a["body"])
        self.assertIn("proj-a", a["subject"])
        self.assertIn("选A或B", a["body"])
        self.assertNotIn("sk-abc1234567890", a["body"])
        self.assertIn("<redacted>", a["body"])

    def test_subject_carries_full_event_id_anti_collision(self):
        # 同 incident 的不同事件（update/recovery）主题因完整事件 ID 而不同。
        f = fact()
        a = digest(f, event_id="a" * 32, kind=KIND_FIRST)
        b = digest(f, event_id="b" * 32, kind=KIND_FIRST)
        self.assertIn("[" + "a" * 32 + "]", a["subject"])
        self.assertNotEqual(a["subject"], b["subject"])

    def test_identity_fields_redacted_and_line_cleaned(self):
        f = fact(project="proj\n sk-abcdefghij12345",
                 condition_key="waiting\nsecret=supersecret789")
        d = digest(f, event_id="ev1", kind=KIND_FIRST)
        self.assertNotIn("sk-abcdefghij12345", d["subject"])
        self.assertNotIn("supersecret789", d["body"])
        self.assertNotIn("\n", d["subject"])
        self.assertIn("<redacted>", d["subject"])
        self.assertIn("<redacted>", d["body"])

    def test_redact_text_strips_credential_shapes(self):
        for bad in ("sk-abcdefghijklmnop", "Bearer abcdefghijk123456",
                    "-----BEGIN RSA PRIVATE KEY-----\nxyz",
                    "password=supersecretvalue"):
            out = redact_text(f"x {bad} y")
            self.assertNotIn(bad, out)
            self.assertIn("<redacted>", out)

    def test_redact_fields_drops_non_whitelisted_and_sensitive(self):
        out = redact_fields({"project": "p", "event_id": "e",
                             "api_key": "secret", "chat_log": "full",
                             "password": "x"})
        self.assertEqual(out, {"project": "p", "event_id": "e"})

    def test_backoff_bounded(self):
        self.assertEqual(backoff_ms(1, retry_min_ms=10_000,
                                    retry_max_ms=80_000), 10_000)
        self.assertEqual(backoff_ms(2, retry_min_ms=10_000,
                                    retry_max_ms=80_000), 20_000)
        self.assertEqual(backoff_ms(5, retry_min_ms=10_000,
                                    retry_max_ms=80_000), 80_000)


# ============================================================================
# 服务层：M01–M12
# ============================================================================
class ServiceM01DefaultOff(MailBase):
    def test_selecting_channel_is_not_authorization(self):
        svc = self.service()
        out = svc.enqueue_incident(fact())
        self.assertEqual(out["result"], "pending-auth")
        self.assertEqual(out["reason"], POLICY_DISABLED)
        self.assertEqual(self.item(out["item_id"])["status"], STATUS_PENDING_AUTH)
        self.assertEqual(self.transport.sent, [])  # 一封不发（M01）
        self.assertEqual(svc.policy_status()["state"], POLICY_DISABLED)

    def test_no_send_and_no_business_touch(self):
        svc = self.service()
        svc.enqueue_incident(fact())
        self.assertEqual(self.business_rows("notifications"), [])
        self.assertEqual(self.business_rows("alerts"), [])

    def test_promote_after_authorization(self):
        # 先默认关闭登记 → 授权到位 → 同一事实再观察 → 升级并发送（M01→M02）
        svc_off = self.service()
        first = svc_off.enqueue_incident(fact())
        item_id = first["item_id"]
        self.assertEqual(self.item(item_id)["status"], STATUS_PENDING_AUTH)
        svc_on = self.service(enabled_policy())
        promoted = svc_on.enqueue_incident(fact())
        self.assertEqual(promoted["result"], "enqueued")
        self.assertTrue(promoted.get("promoted"))
        self.assertEqual(self.item(item_id)["status"], STATUS_PENDING)
        out = svc_on.dispatch_once(owner="sidecar")
        self.assertEqual(out["result"], "accepted")
        self.assertEqual(len(self.transport.sent), 1)


class ServiceM02Scope(MailBase):
    def test_in_scope_auto_send(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        self.assertEqual(out["result"], "enqueued")
        self.assertEqual(svc.dispatch_once(owner="sidecar")["result"], "accepted")
        self.assertEqual(len(self.transport.sent), 1)
        self.assertEqual(svc.dispatch_once(owner="sidecar")["result"],
                         "nothing-due")

    def test_out_of_scope_project_never_sends(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact(project="proj-b"))
        self.assertEqual(out["result"], "pending-auth")
        self.assertEqual(out["reason"], "project-out-of-scope")
        self.assertEqual(svc.dispatch_once(owner="sidecar")["result"],
                         "nothing-due")
        self.assertEqual(self.transport.sent, [])

    def test_event_kind_out_of_scope(self):
        policy = enabled_policy(
            event_kinds=frozenset({KIND_FIRST, KIND_UPDATE, KIND_ESCALATION}))
        svc = self.service(policy)
        svc.enqueue_incident(fact())
        svc.dispatch_once(owner="sidecar")
        out = svc.enqueue_incident(fact(recovered=True, condition_version=2))
        self.assertEqual(out["result"], "pending-auth")
        self.assertEqual(out["reason"], "kind-out-of-scope")

    def test_recipient_change_requires_reauth(self):
        svc = self.service(enabled_policy())  # 只授权 op-primary
        out = svc.enqueue_incident(fact(recipient_ref="other-operator"))
        self.assertEqual(out["result"], "pending-auth")
        self.assertEqual(out["reason"], "recipient-out-of-scope")


class ServiceM03Isolation(MailBase):
    def test_sender_failure_never_raises_and_leaves_business(self):
        self.conn.execute(
            "INSERT INTO tasks(id, project, title, objective, acceptance, status, "
            "write_scope, priority, next_check_at, created_at, updated_at, metadata) "
            "VALUES('T-1','proj-a','x','','[]','open','[]',0,1,1,1,'{}')")
        self.conn.commit()
        svc = self.service(enabled_policy())
        svc.enqueue_incident(fact())
        self.transport.send_result = MailUnknownError("timeout")
        out = svc.dispatch_once(owner="sidecar")
        self.assertEqual(out["result"], "submit-unknown")  # 不抛、不阻塞
        row = self.business_rows("tasks")[0]
        self.assertEqual(row["status"], "open")
        self.assertEqual(row["updated_at"], 1)

    def test_no_model_wake_or_login_surface(self):
        svc = self.service(enabled_policy())
        svc.enqueue_incident(fact())
        svc.dispatch_once(owner="sidecar")
        self.assertFalse(hasattr(svc, "prompt"))
        self.assertFalse(hasattr(self.transport, "wake"))


class ServiceM04AuthRevocation(MailBase):
    def test_revoked_policy_blocks_pending(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        svc._policy = EmailPolicy()  # 模拟撤销（父进程接线处替换配置）
        res = svc.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "auth-blocked")
        self.assertEqual(self.item(out["item_id"])["status"], STATUS_AUTH_BLOCKED)
        self.assertEqual(self.transport.sent, [])

    def test_auth_error_marks_blocked_no_auto_login(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailAuthError("credential-invalid")
        res = svc.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "auth-blocked")
        item = self.item(out["item_id"])
        self.assertEqual(item["status"], STATUS_AUTH_BLOCKED)
        self.assertEqual(item["failure_class"], "auth")
        self.assertEqual(svc.dispatch_once(owner="sidecar")["result"],
                         "nothing-due")


class ServiceM05M06Dedupe(MailBase):
    def test_normal_pause_never_nags(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact(condition_kind=CONDITION_PAUSE))
        self.assertEqual(out["result"], "no-new-event")
        self.assertEqual(out["reason"], "normal-pause-no-nag")
        self.assertEqual(self.outbox.list_all(), [])
        self.assertEqual(self.transport.sent, [])

    def test_duplicate_observation_only_refreshes_summary(self):
        svc = self.service(enabled_policy())
        svc.enqueue_incident(fact(observed_at=1_000_000))
        self.clock.advance(wall=5_000)
        second = svc.enqueue_incident(fact(observed_at=1_005_000))
        self.assertEqual(second["result"], "no-new-event")
        items = self.outbox.list_all()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["last_observed_at"], 1_005_000)
        self.assertNotEqual(items[0]["status"], STATUS_SUBMIT_UNKNOWN)

    def test_condition_change_bounded_new_event(self):
        svc = self.service(enabled_policy())
        svc.enqueue_incident(fact(condition_version=1))
        svc.dispatch_once(owner="sidecar")
        upd = svc.enqueue_incident(fact(condition_version=2))
        self.assertEqual(upd["result"], "enqueued")
        self.assertEqual(upd["kind"], KIND_UPDATE)
        svc.dispatch_once(owner="sidecar")
        self.assertEqual(len(self.transport.sent), 2)
        self.assertNotEqual(upd["item_id"],
                            self.outbox.list_all()[0]["item_id"])

    def test_escaped_event_identity_stable_across_restart(self):
        svc = self.service(enabled_policy())
        svc.enqueue_incident(fact())
        self.conn.close()
        self.conn = init(self.home)
        self.outbox = EmailOutbox(self.conn, self.clock)
        svc2 = self.service(enabled_policy())
        again = svc2.enqueue_incident(fact())
        self.assertEqual(again["result"], "no-new-event")
        self.assertEqual(len(self.outbox.list_all()), 1)


class ServiceM07Recovery(MailBase):
    def test_recovery_echoes_original_incident_once(self):
        svc = self.service(enabled_policy())
        svc.enqueue_incident(fact())
        svc.dispatch_once(owner="sidecar")
        rec = svc.enqueue_incident(fact(recovered=True, condition_version=2))
        self.assertEqual(rec["result"], "enqueued")
        self.assertEqual(rec["kind"], KIND_RECOVERY)
        svc.dispatch_once(owner="sidecar")
        again = svc.enqueue_incident(fact(recovered=True, condition_version=2))
        self.assertEqual(again["result"], "no-new-event")
        self.assertEqual(again["reason"], "duplicate-recovery")
        self.assertEqual(len(self.transport.sent), 2)

    def test_orphan_recovery_suppressed(self):
        svc = self.service(enabled_policy())
        first = svc.enqueue_incident(fact())
        out = svc.enqueue_incident(fact(recovered=True, condition_version=2))
        self.assertEqual(out["result"], "suppressed")
        self.assertEqual(out["expired"], 1)
        self.assertEqual(self.item(first["item_id"])["status"],
                         STATUS_EXPIRED_SUPPRESSED)
        self.assertEqual(len(self.outbox.list_all()), 1)
        self.assertEqual(self.transport.sent, [])

    def test_recovery_does_not_claim_task_accepted(self):
        self.conn.execute(
            "INSERT INTO tasks(id, project, title, objective, acceptance, status, "
            "write_scope, priority, next_check_at, created_at, updated_at, metadata) "
            "VALUES('T-2','proj-a','x','','[]','open','[]',0,1,1,1,'{}')")
        self.conn.commit()
        svc = self.service(enabled_policy())
        svc.enqueue_incident(fact())
        svc.dispatch_once(owner="sidecar")
        svc.enqueue_incident(fact(recovered=True, condition_version=2))
        row = self.business_rows("tasks")[0]
        self.assertEqual(row["status"], "open")  # 不自动 accepted


class ServiceM10UnknownReceipt(MailBase):
    def _unknown(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailUnknownError("send-timeout")
        res = svc.dispatch_once(owner="sidecar")
        assert res["result"] == "submit-unknown"
        return svc, out

    def test_unknown_never_resends(self):
        svc, out = self._unknown()
        self.assertEqual(self.item(out["item_id"])["status"], STATUS_SUBMIT_UNKNOWN)
        self.assertEqual(svc.dispatch_once(owner="sidecar")["result"],
                         "nothing-due")
        self.assertEqual(len(self.transport.sent), 1)

    def test_unique_receipt_upgrades_to_accepted(self):
        svc, out = self._unknown()
        item = self.item(out["item_id"])
        self.transport.search_result = [{
            "message_id": "msg_123", "subject": item["subject"],
            "from": {"email": "ops@example.test"},
            "to": [{"email": "operator@example.test"}],
            "created_at": "1970-01-01T00:16:40Z", "snippet": "",
        }]
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "verified-accepted")
        self.assertEqual(res["external_ref"], "msg_123")
        self.assertEqual(self.item(out["item_id"])["status"], STATUS_ACCEPTED)

    def test_no_match_keeps_unknown(self):
        svc, out = self._unknown()
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "no-match")  # 无匹配 ≠ 未发送
        self.assertEqual(self.item(out["item_id"])["status"], STATUS_SUBMIT_UNKNOWN)

    def test_ambiguous_keeps_unknown(self):
        svc, out = self._unknown()
        item = self.item(out["item_id"])
        self.transport.search_result = [{
            "message_id": "msg_a", "subject": item["subject"],
            "from": {"email": "ops@example.test"},
            "to": [{"email": "operator@example.test"}], "created_at": "",
            "snippet": ""},
            {"message_id": "msg_b", "subject": item["subject"],
             "from": {"email": "other@example.test"},
             "to": [{"email": "operator@example.test"}], "created_at": "",
             "snippet": ""},
        ]
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "ambiguous")
        self.assertEqual(self.item(out["item_id"])["status"], STATUS_SUBMIT_UNKNOWN)

    def test_mismatched_from_is_ambiguous(self):
        svc, out = self._unknown()
        item = self.item(out["item_id"])
        self.transport.search_result = [{
            "message_id": "msg_x", "subject": item["subject"],
            "from": {"email": "stranger@example.net"},
            "to": [{"email": "operator@example.test"}], "created_at": "",
            "snippet": "",
        }]
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "ambiguous")


class ServiceStrictReceipt(MailBase):
    """M10：缺 from/ID/created_at 或时间窗外 → 不能 verified-accepted。"""

    def _unknown(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailUnknownError("timeout")
        svc.dispatch_once(owner="sidecar")
        return svc, out

    def _hit(self, item, **over):
        base = {"message_id": "msg_1", "subject": item["subject"],
                "from": {"email": "ops@example.test"},
                "to": [{"email": "operator@example.test"}],
                "created_at": "1970-01-01T00:16:40Z", "snippet": ""}
        base.update(over)
        return base

    def test_missing_message_id_not_unique(self):
        svc, out = self._unknown()
        item = self.item(out["item_id"])
        self.transport.search_result = [self._hit(item, message_id="")]
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "ambiguous")
        self.assertEqual(self.item(out["item_id"])["status"], STATUS_SUBMIT_UNKNOWN)

    def test_missing_from_not_unique(self):
        svc, out = self._unknown()
        item = self.item(out["item_id"])
        self.transport.search_result = [self._hit(item, **{"from": {}})]
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "ambiguous")

    def test_missing_created_at_not_unique(self):
        svc, out = self._unknown()
        item = self.item(out["item_id"])
        self.transport.search_result = [self._hit(item, created_at="")]
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "ambiguous")

    def test_out_of_window_created_at_not_unique(self):
        svc, out = self._unknown()
        item = self.item(out["item_id"])
        self.transport.search_result = [self._hit(item,
                                                  created_at="2026-01-01T00:00:00Z")]
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "ambiguous")

    def test_subject_mismatch_is_no_match(self):
        svc, out = self._unknown()
        item = self.item(out["item_id"])
        self.transport.search_result = [self._hit(item, subject="其他主题")]
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "no-match")

    def test_wrong_recipient_is_no_match(self):
        svc, out = self._unknown()
        item = self.item(out["item_id"])
        self.transport.search_result = [self._hit(
            item, to=[{"email": "someone@example.net"}])]
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "no-match")


class ServiceQueryAuthorization(MailBase):
    """M10：最小回执查询授权独立于发送授权；拒绝时不耗预算。"""

    def _unknown_with(self, policy):
        svc = self.service(policy)
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailUnknownError("timeout")
        svc.dispatch_once(owner="sidecar")
        return svc, out

    def test_verify_denied_when_queries_not_authorized(self):
        policy = enabled_policy(receipt_queries_authorized=False)
        svc, out = self._unknown_with(policy)
        self.assertEqual(self.item(out["item_id"])["status"], STATUS_SUBMIT_UNKNOWN)
        self.transport.search_result = [{
            "message_id": "msg_1", "subject": self.item(out["item_id"])["subject"],
            "from": {"email": "ops@example.test"},
            "to": [{"email": "operator@example.test"}],
            "created_at": "1970-01-01T00:16:40Z", "snippet": "",
        }]
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "queries-not-authorized")
        self.assertEqual(res["reason"], "receipt-queries-not-authorized")
        # 拒绝不耗回执预算、不发外部查询
        self.assertEqual(self.item(out["item_id"])["receipt_checks"], 0)
        self.assertEqual(self.transport.searches, [])

    def test_disabled_policy_also_denies_query(self):
        # 行已 submit-unknown（全授权造），随后策略完全关闭 → verify 拒绝且不耗预算。
        svc, out = self._unknown_with(enabled_policy())
        self.assertEqual(self.item(out["item_id"])["status"], STATUS_SUBMIT_UNKNOWN)
        svc._policy = EmailPolicy()  # default disabled
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "queries-not-authorized")
        self.assertEqual(self.item(out["item_id"])["receipt_checks"], 0)


class ServiceBindingFrozen(MailBase):
    def test_sender_ref_change_blocks_send(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        changed = enabled_policy(sender=Sender("oc-ops-changed",
                                               "ops@example.test"))
        svc._policy = changed
        res = svc.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "auth-blocked")
        self.assertEqual(res["reason"], "sender-ref-changed")
        self.assertEqual(self.transport.sent, [])

    def test_recipient_address_change_blocks_send(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        changed = enabled_policy(
            recipients=(Recipient("op-primary", "new@example.net"),))
        svc._policy = changed
        res = svc.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "auth-blocked")
        self.assertEqual(res["reason"], "recipient-config-changed")
        self.assertEqual(self.transport.sent, [])

    def test_sender_identity_change_blocks_send(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        changed = enabled_policy(sender=Sender("oc-ops", "other@example.net"))
        svc._policy = changed
        res = svc.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "auth-blocked")
        self.assertEqual(res["reason"], "sender-config-changed")

    def test_old_unknown_cannot_be_proven_by_new_config(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailUnknownError("timeout")
        svc.dispatch_once(owner="sidecar")
        self.assertEqual(self.item(out["item_id"])["status"], STATUS_SUBMIT_UNKNOWN)
        changed = enabled_policy(sender=Sender("oc-ops", "other@example.net"))
        svc._policy = changed
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "auth-blocked")
        self.assertEqual(res["reason"], "sender-config-changed")
        # 配置不符在 claim 预算前拒绝：不消耗回执预算
        self.assertEqual(self.item(out["item_id"])["receipt_checks"], 0)


class ServiceQueuedStrict(MailBase):
    def test_send_returning_none_is_submit_unknown(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        self.transport.send_result = None
        res = svc.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "submit-unknown")
        self.assertEqual(self.item(out["item_id"])["status"], STATUS_SUBMIT_UNKNOWN)

    def test_send_returning_dict_without_queued_true_is_unknown(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        self.transport.send_result = {"accepted": True}  # 无 queued is True
        res = svc.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "submit-unknown")
        self.assertEqual(res["reason"], "queued-not-confirmed")

    def test_unexpected_exception_keeps_unknown_no_raise(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        self.transport.send_result = RuntimeError("boom")
        res = svc.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "submit-unknown")
        self.assertTrue(res["reason"].startswith("unexpected:"))
        self.assertEqual(self.item(out["item_id"])["status"], STATUS_SUBMIT_UNKNOWN)
        # 后续不因意外异常重发
        self.assertEqual(svc.dispatch_once(owner="sidecar")["result"],
                         "nothing-due")


class ServiceM11BoundedRetry(MailBase):
    def test_transient_backoff_then_retry(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailTransientNotSubmittedError("refused")
        res = svc.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "retry-scheduled")
        item = self.item(out["item_id"])
        self.assertEqual(item["status"], STATUS_RETRY_WAIT)
        self.assertIsNone(item["possible_submitted_at"])  # 已证未提交
        next_at = item["next_attempt_at"]
        self.clock.advance(wall=min(999, next_at - self.clock.now_ms()))
        self.assertEqual(svc.dispatch_once(owner="sidecar")["result"],
                         "nothing-due")
        self.clock.advance(wall=max(1, next_at - self.clock.now_ms() + 1))
        self.transport.send_result = {"queued": True}
        res2 = svc.dispatch_once(owner="sidecar")
        self.assertEqual(res2["result"], "accepted")
        self.assertEqual(len(self.transport.sent), 2)

    def test_budget_exhausted_becomes_permanent_and_business_kept(self):
        self.conn.execute(
            "INSERT INTO tasks(id, project, title, objective, acceptance, status, "
            "write_scope, priority, next_check_at, created_at, updated_at, metadata) "
            "VALUES('T-3','proj-a','x','','[]','open','[]',0,1,1,1,'{}')")
        self.conn.commit()
        policy = enabled_policy(max_attempts=2)
        svc = self.service(policy)
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailTransientNotSubmittedError("refused")
        r1 = svc.dispatch_once(owner="sidecar")
        self.assertEqual(r1["result"], "retry-scheduled")
        self.clock.advance(wall=policy.retry_max_ms + 1)
        r2 = svc.dispatch_once(owner="sidecar")
        self.assertEqual(r2["result"], "permanent-failed")
        self.assertEqual(r2["reason"], "transient-budget-exhausted")
        item = self.item(out["item_id"])
        self.assertEqual(item["status"], STATUS_PERMANENT_FAILED)
        row = self.business_rows("tasks")[0]
        self.assertEqual(row["status"], "open")
        self.assertIsNone(row["current_attempt_id"])

    def test_rate_limit_honors_retry_after(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailRateLimitError(
            "limited", retry_after_ms=5_000)
        res = svc.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "rate-limited")
        item = self.item(out["item_id"])
        self.assertEqual(item["status"], STATUS_RATE_LIMITED)
        self.assertEqual(item["next_attempt_at"],
                         self.clock.now_ms() + 5_000)
        self.assertIsNone(item["possible_submitted_at"])

    def test_rate_limit_retry_after_not_shortened(self):
        # retry_max 很小也不能压短服务给出的 retry-after（M11）。
        policy = enabled_policy(retry_max_ms=1_000)
        svc = self.service(policy)
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailRateLimitError(
            "limited", retry_after_ms=10_000)
        res = svc.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "rate-limited")
        self.assertEqual(res["next_attempt_at"],
                         self.clock.now_ms() + 10_000)  # 未被压至 1000

    def test_frozen_max_attempts_not_reset_on_reconfig(self):
        svc = self.service(enabled_policy(max_attempts=2))
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailTransientNotSubmittedError("refused")
        svc.dispatch_once(owner="sidecar")
        # 即使把 policy 的 max_attempts 改大，行内冻结 cap 仍生效（第 2 次即永久）
        svc._policy = enabled_policy(max_attempts=99)
        self.clock.advance(wall=80_001)
        r2 = svc.dispatch_once(owner="sidecar")
        self.assertEqual(r2["result"], "permanent-failed")

    def test_budget_persists_across_restart(self):
        svc = self.service(enabled_policy(max_attempts=3))
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailTransientNotSubmittedError("refused")
        svc.dispatch_once(owner="sidecar")
        self.conn.close()
        self.conn = init(self.home)
        self.outbox = EmailOutbox(self.conn, self.clock)
        svc2 = self.service(enabled_policy(max_attempts=3))
        self.assertEqual(self.item(out["item_id"])["attempts"], 1)


class ServiceM12ExpiryAndLate(MailBase):
    def test_first_not_yet_possible_suppressed_on_recovery(self):
        svc = self.service(enabled_policy())
        first = svc.enqueue_incident(fact())
        self.assertEqual(self.item(first["item_id"])["status"], STATUS_PENDING)
        out = svc.enqueue_incident(fact(recovered=True, condition_version=2))
        self.assertEqual(out["result"], "suppressed")
        self.assertEqual(self.item(first["item_id"])["status"],
                         STATUS_EXPIRED_SUPPRESSED)
        self.assertEqual(len(self.outbox.list_all()), 1)
        self.assertEqual(self.transport.sent, [])

    def test_claimed_possible_not_withdrawable(self):
        svc = self.service(enabled_policy())
        first = svc.enqueue_incident(fact())
        claimed = self.outbox.claim(first["item_id"], owner="sidecar",
                                    now=self.clock.now_ms())
        self.assertTrue(claimed["allowed"])
        self.assertIsNotNone(self.item(first["item_id"])["possible_submitted_at"])
        out = svc.enqueue_incident(fact(recovered=True, condition_version=2))
        self.assertEqual(out["result"], "enqueued")
        self.assertEqual(out["kind"], KIND_RECOVERY)
        self.assertEqual(self.item(first["item_id"])["status"], STATUS_CLAIMED)

    def test_late_receipt_updates_original_only(self):
        svc = self.service(enabled_policy())
        first = svc.enqueue_incident(fact())
        self.transport.send_result = MailUnknownError("timeout")
        svc.dispatch_once(owner="sidecar")
        upd = svc.enqueue_incident(fact(condition_version=2))
        self.assertEqual(upd["result"], "enqueued")
        item = self.item(first["item_id"])
        self.transport.search_result = [{
            "message_id": "msg_late", "subject": item["subject"],
            "from": {"email": "ops@example.test"},
            "to": [{"email": "operator@example.test"}],
            "created_at": "1970-01-01T00:16:40Z", "snippet": "",
        }]
        res = svc.verify_receipt(first["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "verified-accepted")
        self.assertEqual(self.item(first["item_id"])["status"], STATUS_ACCEPTED)
        self.assertEqual(self.item(upd["item_id"])["status"], STATUS_PENDING)
        self.assertEqual(len(self.outbox.list_all()), 2)


class ServiceDispatchSingleWinner(MailBase):
    def test_two_dispatch_once_race_single_send(self):
        svc_a = self.service(enabled_policy())
        svc_b = self.service(enabled_policy())
        out = svc_a.enqueue_incident(fact())
        item_id = out["item_id"]
        r1 = svc_a.dispatch_once(item_id=item_id, owner="sidecar-a")
        r2 = svc_b.dispatch_once(item_id=item_id, owner="sidecar-b")
        winners = [r for r in (r1, r2) if r.get("result") == "accepted"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(self.transport.sent), 1)

    def test_no_credit_to_business_ack(self):
        svc = self.service(enabled_policy())
        svc.enqueue_incident(fact())
        svc.dispatch_once(owner="sidecar")
        self.assertEqual(self.business_rows("notifications"), [])
        self.assertEqual(self.business_rows("alerts"), [])


class ServiceExtraGuards(MailBase):
    def test_policy_status_never_exposes_addresses(self):
        svc = self.service(enabled_policy())
        view = svc.policy_status()
        self.assertEqual(view["state"], POLICY_ENABLED)
        self.assertNotIn("operator@example.test", str(view))
        self.assertNotIn("ops@example.test", str(view))
        self.assertEqual(view["recipient_refs"], ["op-primary"])

    def test_escalation_point_bounded_new_event(self):
        svc = self.service(enabled_policy())
        svc.enqueue_incident(fact(condition_version=1))
        svc.dispatch_once(owner="sidecar")
        esc = svc.enqueue_incident(fact(condition_version=1, escalation_level=2))
        self.assertEqual(esc["result"], "enqueued")
        self.assertEqual(esc["kind"], KIND_ESCALATION)
        svc.dispatch_once(owner="sidecar")
        self.assertEqual(len(self.transport.sent), 2)
        again = svc.enqueue_incident(fact(condition_version=1, escalation_level=2))
        self.assertEqual(again["result"], "no-new-event")

    def test_receipt_check_budget_is_bounded(self):
        policy = enabled_policy(receipt_check_limit=2)
        svc = self.service(policy)
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailUnknownError("timeout")
        svc.dispatch_once(owner="sidecar")
        for _ in range(3):
            res = svc.verify_receipt(out["item_id"], owner="sidecar")
        final = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(final["result"], "receipt-check-exhausted")
        item = self.item(out["item_id"])
        self.assertEqual(item["receipt_checks"], 2)  # 网络前原子消耗，跨重启不清零
        self.assertEqual(item["status"], STATUS_SUBMIT_UNKNOWN)

    def test_verify_accepted_row_is_not_applicable(self):
        svc = self.service(enabled_policy())
        out = svc.enqueue_incident(fact())
        svc.dispatch_once(owner="sidecar")
        res = svc.verify_receipt(out["item_id"], owner="sidecar")
        self.assertEqual(res["result"], "not-applicable")

    def test_duplicate_verify_atomic_single_budget(self):
        # 两个观察者对同一行并发 verify：预算各自 +1 但被 limit 封顶（不无限查询）。
        svc = self.service(enabled_policy(receipt_check_limit=2))
        out = svc.enqueue_incident(fact())
        self.transport.send_result = MailUnknownError("timeout")
        svc.dispatch_once(owner="sidecar")
        item_id = out["item_id"]
        c1 = self.outbox.claim_receipt(item_id, owner="o1", now=self.clock.now_ms())
        c2 = self.outbox.claim_receipt(item_id, owner="o2", now=self.clock.now_ms())
        self.assertTrue(c1["allowed"] and c2["allowed"])
        self.assertEqual(self.item(item_id)["receipt_checks"], 2)
        denied = self.outbox.claim_receipt(item_id, owner="o3",
                                           now=self.clock.now_ms())
        self.assertFalse(denied["allowed"])
        self.assertEqual(denied["reason"], "receipt-check-exhausted")


if __name__ == "__main__":
    unittest.main(verbosity=2)

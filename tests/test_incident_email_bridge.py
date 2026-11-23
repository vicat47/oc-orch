#!/usr/bin/env python3
"""incident → 邮件 outbox 隔离桥（roadmap R3 关键安全）。

用真临时 SQLite（WAL，双连接）、FakeClock、可注入 fake transport 覆盖：

- 无收件范围 → 仅本地可见（零 outbox/零发送）；有显式 ref 但邮件禁用 →
  pending-auth、绝不外发；
- 入 outbox 前在同一事务内重验源（坏/未知原 ID → source-recheck-failed 不建行）；
- 真实发送 claim 前在同一事务内重验源（责任关闭竞态 → 拒绝认领/零发送，
  源重验 + 可能提交原子，不是 service 先读后放行）；
- 正常暂停/冷却不催；恢复关联原问题且至多一次；无前置可能提交 → 抑制；
- require_source_binding：真实启用路径可要求源绑定，缺绑定 fail-closed；
- 双连接单一认领、跨连接提交后验证看到最新事实；晚到回执只更新原记录
  不覆盖新事件；reconcile 零网络（独立执行器发送）。
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.adapters.sqlite_db import init  # noqa: E402
from oc_orch.adapters.email_outbox import (  # noqa: E402
    STATUS_ACCEPTED,
    STATUS_EXPIRED_SUPPRESSED,
    STATUS_PENDING,
    STATUS_PENDING_AUTH,
    STATUS_SUBMIT_UNKNOWN,
    EmailOutbox,
)
from oc_orch.adapters.operator_incident import IncidentStore  # noqa: E402
from oc_orch.application.mail_port import MailUnknownError  # noqa: E402
from oc_orch.application.operator_email import OperatorEmailService  # noqa: E402
from oc_orch.application.operator_incident import OperatorIncidentService  # noqa: E402
from oc_orch.domain.operator_email import (  # noqa: E402
    KIND_FIRST,
    KIND_RECOVERY,
    EmailPolicy,
    Recipient,
    Sender,
)
from tests.fakes import FakeClock  # noqa: E402
from tests.test_operator_incident import _resp, _task  # noqa: E402


def _open_wal(home):
    conn = sqlite3.connect(os.path.join(home, "ledger.db"), timeout=15,
                           isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def enabled_policy(**overrides) -> EmailPolicy:
    kwargs = dict(
        enabled=True,
        sender=Sender("oc-ops", "ops@example.test"),
        recipients=(Recipient("op-primary", "operator@example.test"),),
        projects=frozenset({"proj-a"}),
        event_kinds=frozenset({KIND_FIRST, KIND_RECOVERY}),
        unattended=True,
        receipt_queries_authorized=True,
    )
    kwargs.update(overrides)
    return EmailPolicy(**kwargs)


class FakeTransport:
    """可注入结果/异常的窄传输桩（绝不接触真实邮箱/网络）。"""

    def __init__(self):
        self.sent: list[dict] = []
        self.searches: list[dict] = []
        self.send_result: object = {"queued": True}
        self.search_result: object = []

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


class BridgeBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="bridge-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.inc_store = IncidentStore(self.conn, self.clock)
        self.validator = self.inc_store.make_source_validator()
        self.transport = FakeTransport()

    def outbox(self, **kw) -> EmailOutbox:
        kw.setdefault("source_validator", self.validator)
        kw.setdefault("require_source_binding", True)
        return EmailOutbox(self.conn, self.clock, **kw)

    def mail(self, policy: EmailPolicy | None = None) -> OperatorEmailService:
        return OperatorEmailService(self.outbox(), self.transport, self.clock,
                                    policy if policy is not None else EmailPolicy())

    def svc(self, mail=None, recipient_refs=("op-primary",)):
        return OperatorIncidentService(self.inc_store, self.clock,
                                       email=mail, recipient_refs=recipient_refs)


# ============================================================================
# 未配置/禁用 → 零外发
# ============================================================================
class BridgeDefaultOff(BridgeBase):
    def test_no_recipient_refs_local_only_zero_outbox(self):
        _task(self.conn)
        _resp(self.conn)
        svc = self.svc(mail=self.mail(enabled_policy()), recipient_refs=())
        out = svc.reconcile(limit=10)
        self.assertEqual(out["created"], 1)
        detail = [d for d in out["details"] if d["action"] == "create"][0]
        self.assertEqual(detail["email"], "local-only")
        self.assertEqual(self.outbox().list_all(), [])  # 零 outbox
        self.assertEqual(self.transport.sent, [])

    def test_explicit_ref_email_disabled_pending_auth_zero_send(self):
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail(EmailPolicy())  # 默认 disabled
        out = self.svc(mail=mail, recipient_refs=("op-primary",)).reconcile(limit=10)
        self.assertEqual(out["created"], 1)
        self.assertEqual(out["email_pending_auth"], 1)
        rows = self.outbox().list_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], STATUS_PENDING_AUTH)
        self.assertEqual(rows[0]["kind"], KIND_FIRST)
        # 绝不外发：pending-auth 不在可认领状态，执行器 nothing-due
        res = mail.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "nothing-due")
        self.assertEqual(self.transport.sent, [])

    def test_normal_pause_no_outbox_event(self):
        _task(self.conn, status="paused")
        _resp(self.conn)
        svc = self.svc(mail=self.mail(enabled_policy()))
        out = svc.reconcile(limit=10)
        self.assertEqual(out["created"], 1)
        self.assertEqual(out["email_enqueued"], 0)
        self.assertEqual(out["email_pending_auth"], 0)
        self.assertEqual(self.outbox().list_all(), [])


# ============================================================================
# 授权范围内：桥接 + 发送 + 源重验原子性
# ============================================================================
class BridgeAuthorized(BridgeBase):
    def test_first_send_accepted(self):
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail(enabled_policy())
        svc = self.svc(mail=mail)
        out = svc.reconcile(limit=10)
        self.assertEqual(out["email_enqueued"], 1)
        rows = self.outbox().list_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], STATUS_PENDING)
        res = mail.dispatch_once(owner="sidecar")
        self.assertEqual(res["result"], "accepted")
        self.assertEqual(len(self.transport.sent), 1)
        self.assertEqual(self.outbox().get_item(rows[0]["item_id"])["status"],
                         STATUS_ACCEPTED)

    def test_reconcile_never_touches_channel(self):
        # 慢通道不进主循环：reconcile 只写台账/outbox 意图，零发送/零查询。
        _task(self.conn)
        _resp(self.conn)
        svc = self.svc(mail=self.mail(enabled_policy()))
        svc.reconcile(limit=10)
        self.assertEqual(self.transport.sent, [])
        self.assertEqual(self.transport.searches, [])

    def test_claim_recheck_atomic_blocks_stale_source(self):
        # 入 outbox 后、真实发送 claim 前，责任被关闭 → claim 在同一事务内
        # 重验失败 → 拒绝认领、零发送、转可见终态（源重验+claim 原子）。
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail(enabled_policy())
        svc = self.svc(mail=mail)
        svc.reconcile(limit=10)
        item_id = self.outbox().list_all()[0]["item_id"]
        self.conn.execute(
            "UPDATE responsibilities SET closed_at=20000, state='cancelled' "
            "WHERE id='R-1'")
        self.conn.commit()
        res = mail.dispatch_once(item_id=item_id, owner="sidecar")
        self.assertEqual(res["result"], "not-claimed")
        self.assertEqual(res["reason"], "source-stale")
        self.assertEqual(self.transport.sent, [])
        item = self.outbox().get_item(item_id)
        self.assertEqual(item["status"], STATUS_EXPIRED_SUPPRESSED)
        # 不再进入发送调度（不忙循环）
        self.assertEqual(mail.dispatch_once(owner="sidecar")["result"],
                         "nothing-due")

    def test_enqueue_recheck_blocks_stale_source_in_transaction(self):
        # 入 outbox 前重验（同一事务内）：绑定指向已关闭的责任 → 不建行。
        _task(self.conn)
        _resp(self.conn)
        from oc_orch.domain import operator_incident as inc
        src = self.inc_store.list_waiting_human_sources(limit=10)[0][0]
        binding = inc.build_binding(src)
        self.conn.execute(
            "UPDATE responsibilities SET closed_at=20000, state='cancelled' "
            "WHERE id='R-1'")
        self.conn.commit()
        mail = self.mail(enabled_policy())
        fact = {
            "incident_id": "I-manual", "project": "proj-a",
            "incident_type": "waiting-human-responsibility",
            "condition_key": "waiting-human", "condition_version": 1,
            "recipient_ref": "op-primary", "condition_kind": "human-responsibility",
            "first_seen_at": 1_000_000, "observed_at": 1_000_000,
        }
        res = mail.enqueue_incident(fact, source_binding=binding)
        self.assertEqual(res["result"], "source-recheck-failed")
        self.assertEqual(self.outbox().list_all(), [])

    def test_unknown_source_id_fail_closed(self):
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail(enabled_policy())
        fact = {
            "incident_id": "I-404", "project": "proj-a",
            "incident_type": "waiting-human-responsibility",
            "condition_key": "waiting-human", "condition_version": 1,
            "recipient_ref": "op-primary", "condition_kind": "human-responsibility",
            "first_seen_at": 1_000_000, "observed_at": 1_000_000,
        }
        binding = {
            "source_kind": "responsibility", "source_id": "R-404",
            "source_condition_key": "waiting-human", "source_version": "sig-x",
            "source_state": "waiting_human",
            "source_incident_type": "waiting-human-responsibility",
            "source_project": "proj-a", "source_task_id": "T-1",
            "source_session": None, "source_model": None,
        }
        res = mail.enqueue_incident(fact, source_binding=binding)
        self.assertEqual(res["result"], "source-recheck-failed")
        self.assertEqual(self.outbox().list_all(), [])

    def test_require_source_binding_fail_closed(self):
        # 真实启用路径必须携带可重验源绑定：缺绑定不建行（不允许调用方自报）。
        outbox = EmailOutbox(self.conn, self.clock,
                             source_validator=self.validator,
                             require_source_binding=True)
        res = outbox.upsert_item(
            item_id="ev-no-src", base_key="b", event_key="ev-no-src",
            project="proj-a", incident_id="i", incident_type="t",
            kind=KIND_FIRST, condition_version=1, escalation_level=0,
            recipient_ref="op-primary", sender_ref="oc-ops",
            recipient_fingerprint="r", sender_fingerprint="s",
            subject="s", body="b", content_hash="c", status=STATUS_PENDING,
            first_seen_at=1_000_000, observed_at=1_000_000,
            summary={}, max_attempts=3, receipt_limit=3,
            deadline_at=None, now=1_000_000)
        self.assertEqual(res["reason"], "source-recheck-failed")
        self.assertEqual(outbox.list_all(), [])


# ============================================================================
# 恢复：关联原问题 / 至多一次 / 无前置可能提交则抑制
# ============================================================================
class BridgeRecovery(BridgeBase):
    def _reconcile_first(self):
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail(enabled_policy())
        svc = self.svc(mail=mail)
        svc.reconcile(limit=10)
        return mail, svc

    def test_orphan_recovery_suppressed_pending_first(self):
        # 首封尚未可能提交即恢复 → 抑制（不发送孤立恢复邮件）
        mail, svc = self._reconcile_first()
        item_id = self.outbox().list_all()[0]["item_id"]
        self.conn.execute(
            "UPDATE responsibilities SET closed_at=20000, state='cancelled' "
            "WHERE id='R-1'")
        self.conn.execute(
            "UPDATE tasks SET status='cancelled', closed_at=25000 WHERE id='T-1'")
        self.conn.commit()
        out = svc.reconcile(limit=10)
        self.assertEqual(out["recovered"], 1)
        self.assertEqual(self.outbox().get_item(item_id)["status"],
                         STATUS_EXPIRED_SUPPRESSED)
        self.assertEqual(self.transport.sent, [])

    def test_recovery_event_once_after_prior_possible_submit(self):
        mail, svc = self._reconcile_first()
        first_id = self.outbox().list_all()[0]["item_id"]
        mail.dispatch_once(item_id=first_id, owner="sidecar")  # accepted
        self.assertEqual(len(self.transport.sent), 1)
        self.conn.execute(
            "UPDATE responsibilities SET closed_at=20000, state='cancelled' "
            "WHERE id='R-1'")
        self.conn.execute(
            "UPDATE tasks SET status='cancelled', closed_at=25000 WHERE id='T-1'")
        self.conn.commit()
        out = svc.reconcile(limit=10)
        self.assertEqual(out["recovered"], 1)
        self.assertEqual(out["email_enqueued"], 1)
        rows = self.outbox().list_all()
        kinds = sorted(r["kind"] for r in rows)
        self.assertEqual(kinds, [KIND_FIRST, KIND_RECOVERY])
        recovery = [r for r in rows if r["kind"] == KIND_RECOVERY][0]
        self.assertEqual(recovery["status"], STATUS_PENDING)
        # 恢复只登记一次：再跑一轮不再建新恢复事件
        again = svc.reconcile(limit=10)
        self.assertEqual(again["recovered"], 0)
        self.assertEqual(again["email_enqueued"], 0)

    def test_late_receipt_updates_original_only(self):
        # 晚到回执只更新原发送记录，不覆盖新恢复事件
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail(enabled_policy())
        svc = self.svc(mail=mail)
        svc.reconcile(limit=10)
        first_id = self.outbox().list_all()[0]["item_id"]
        self.transport.send_result = MailUnknownError("timeout")
        mail.dispatch_once(item_id=first_id, owner="sidecar")
        self.assertEqual(self.outbox().get_item(first_id)["status"],
                         STATUS_SUBMIT_UNKNOWN)
        self.conn.execute(
            "UPDATE responsibilities SET closed_at=20000, state='cancelled' "
            "WHERE id='R-1'")
        self.conn.execute(
            "UPDATE tasks SET status='cancelled', closed_at=25000 WHERE id='T-1'")
        self.conn.commit()
        svc.reconcile(limit=10)
        recovery = [r for r in self.outbox().list_all()
                    if r["kind"] == KIND_RECOVERY][0]
        recovery_id = recovery["item_id"]
        item = self.outbox().get_item(first_id)
        self.transport.send_result = {"queued": True}
        self.transport.search_result = [{
            "message_id": "msg_late", "subject": item["subject"],
            "from": {"email": "ops@example.test"},
            "to": [{"email": "operator@example.test"}],
            "created_at": "1970-01-01T00:16:40Z", "snippet": "",
        }]
        res = mail.verify_receipt(first_id, owner="sidecar")
        self.assertEqual(res["result"], "verified-accepted")
        self.assertEqual(self.outbox().get_item(first_id)["status"],
                         STATUS_ACCEPTED)
        # 新恢复事件不被晚到回执覆盖
        self.assertEqual(self.outbox().get_item(recovery_id)["status"],
                         STATUS_PENDING)


# ============================================================================
# 双连接 / 并发：源重验读最新提交；单一发送权
# ============================================================================
class BridgeConcurrency(BridgeBase):
    def _second_conn(self):
        return _open_wal(self.home)

    def test_cross_connection_sees_committed_close_at_claim(self):
        # 另一个连接已提交责任关闭 → 本连接 claim 事务内重验读到最新事实并拒绝
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail(enabled_policy())
        svc = self.svc(mail=mail)
        svc.reconcile(limit=10)
        item_id = self.outbox().list_all()[0]["item_id"]
        conn_b = self._second_conn()
        self.addCleanup(conn_b.close)
        conn_b.execute(
            "UPDATE responsibilities SET closed_at=20000, state='cancelled' "
            "WHERE id='R-1'")
        conn_b.commit()
        res = mail.dispatch_once(item_id=item_id, owner="sidecar")
        self.assertEqual(res["reason"], "source-stale")
        self.assertEqual(self.transport.sent, [])

    def test_single_claim_winner_across_connections(self):
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail(enabled_policy())
        svc = self.svc(mail=mail)
        svc.reconcile(limit=10)
        item_id = self.outbox().list_all()[0]["item_id"]
        conn_b = self._second_conn()
        self.addCleanup(conn_b.close)
        outbox_b = EmailOutbox(conn_b, self.clock, source_validator=self.validator,
                               require_source_binding=True)
        r1 = self.outbox().claim(item_id, owner="sidecar-a",
                                 now=self.clock.now_ms())
        r2 = outbox_b.claim(item_id, owner="sidecar-b",
                            now=self.clock.now_ms())
        self.assertTrue(r1["allowed"])
        self.assertFalse(r2["allowed"])
        self.assertEqual(r2["reason"], "not-claimable")


if __name__ == "__main__":
    unittest.main(verbosity=2)


# ============================================================================
# 父核收补充：无 validator 拒伪造绑定 / 暂停 claim 暂停后恢复 / 期限延后失效 /
# 恢复 claim 重验底层 / 跨项目篡改 / 旧未知不因新绑定重发
# ============================================================================
class ReviewValidatorGates(BridgeBase):
    def test_require_binding_without_validator_rejected(self):
        outbox = EmailOutbox(self.conn, self.clock, require_source_binding=True)
        binding = {
            "source_kind": "responsibility", "source_id": "R-1",
            "source_condition_key": "waiting-human", "source_version": "sig-forged",
            "source_state": "waiting_human",
            "source_incident_type": "waiting-human-responsibility",
            "source_project": "proj-a", "source_task_id": "T-1",
            "source_session": None, "source_model": None,
        }
        res = outbox.upsert_item(
            item_id="ev-forged", base_key="b", event_key="ev-forged",
            project="proj-a", incident_id="i", incident_type="t",
            kind=KIND_FIRST, condition_version=1, escalation_level=0,
            recipient_ref="op-primary", sender_ref="oc-ops",
            recipient_fingerprint="r", sender_fingerprint="s",
            subject="s", body="b", content_hash="c", status=STATUS_PENDING,
            first_seen_at=1_000_000, observed_at=1_000_000,
            summary={}, max_attempts=3, receipt_limit=3,
            deadline_at=None, now=1_000_000, source_binding=binding)
        self.assertEqual(res["reason"], "source-recheck-failed")
        self.assertEqual(res["recheck_reason"], "source-validator-unavailable")
        self.assertEqual(outbox.list_all(), [])

    def test_cross_project_tamper_refused_at_enqueue(self):
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail(enabled_policy())
        from oc_orch.domain import operator_incident as inc
        src = self.inc_store.list_waiting_human_sources(limit=10)[0][0]
        binding = inc.build_binding(src)
        fact = {
            "incident_id": "I-x", "project": "proj-B",  # 与绑定 project 不一致
            "incident_type": "waiting-human-responsibility",
            "condition_key": "waiting-human", "condition_version": 1,
            "recipient_ref": "op-primary", "condition_kind": "human-responsibility",
            "first_seen_at": 1_000_000, "observed_at": 1_000_000,
        }
        res = mail.enqueue_incident(fact, source_binding=binding)
        self.assertEqual(res["result"], "source-recheck-failed")

    def test_old_unknown_not_resent_after_condition_change(self):
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail(enabled_policy())
        from oc_orch.domain import operator_incident as inc
        src = self.inc_store.list_waiting_human_sources(limit=10)[0][0]
        binding = inc.build_binding(src)
        fact = {
            "incident_id": "I-x", "project": "proj-a",
            "incident_type": "waiting-human-responsibility",
            "condition_key": "waiting-human", "condition_version": 1,
            "recipient_ref": "op-primary", "condition_kind": "human-responsibility",
            "first_seen_at": 1_000_000, "observed_at": 1_000_000,
        }
        mail.enqueue_incident(fact, source_binding=binding)
        item_id = self.outbox().list_all()[0]["item_id"]
        self.conn.execute("UPDATE responsibilities SET condition_key='k-new' WHERE id='R-1'")
        self.conn.commit()
        res = mail.dispatch_once(item_id=item_id, owner="sidecar")
        self.assertEqual(res["result"], "not-claimed")
        self.assertEqual(res["reason"], "source-stale")
        self.assertEqual(self.transport.sent, [])


class ReviewClaimGate(BridgeBase):
    def test_paused_project_claim_refused_no_terminal_then_resume_sends(self):
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail(enabled_policy())
        svc = self.svc(mail=mail)
        svc.reconcile(limit=10)
        item_id = self.outbox().list_all()[0]["item_id"]
        from tests.test_operator_incident import _set_project_paused
        _set_project_paused(self.conn, self.clock, session="ses-1")
        res = mail.dispatch_once(item_id=item_id, owner="sidecar")
        self.assertEqual(res["result"], "not-claimed")
        self.assertEqual(res["reason"], "source-paused")
        self.assertEqual(self.transport.sent, [])
        item = self.outbox().get_item(item_id)
        self.assertEqual(item["status"], STATUS_PENDING)  # 不转终态、不忙写
        from oc_orch.adapters.coordinator_control import CoordinatorControlLedger
        control = CoordinatorControlLedger(self.conn, self.clock)
        control.resume("proj-a", session_id="ses-1", operation_id="op-resume-1",
                       reason="test", expected_revision=1)
        res2 = mail.dispatch_once(item_id=item_id, owner="sidecar")
        self.assertEqual(res2["result"], "accepted")
        self.assertEqual(len(self.transport.sent), 1)

    def test_deadline_extension_invalidates_stale_due_email(self):
        _task(self.conn)
        _resp(self.conn, state="waiting_dependency", wait_kind="dependency",
              condition="dep", deadline=50, next_check=10)
        mail = self.mail(enabled_policy())
        svc = self.svc(mail=mail)
        svc.reconcile(limit=10)
        rows = self.outbox().list_all()
        self.assertEqual(len(rows), 1)
        item_id = rows[0]["item_id"]
        self.conn.execute(
            "UPDATE responsibilities SET deadline=999999999 WHERE id='R-1'")
        self.conn.commit()
        res = mail.dispatch_once(item_id=item_id, owner="sidecar")
        self.assertEqual(res["result"], "not-claimed")
        self.assertEqual(res["reason"], "source-stale")
        self.assertEqual(self.transport.sent, [])
        self.assertEqual(self.outbox().get_item(item_id)["status"],
                         STATUS_EXPIRED_SUPPRESSED)

    def test_recovery_claim_rechecks_underlying_resolved(self):
        _task(self.conn)
        _resp(self.conn)
        mail = self.mail(enabled_policy())
        svc = self.svc(mail=mail)
        svc.reconcile(limit=10)
        first_id = self.outbox().list_all()[0]["item_id"]
        mail.dispatch_once(item_id=first_id, owner="sidecar")  # accepted
        self.conn.execute(
            "UPDATE responsibilities SET closed_at=20000, state='cancelled' "
            "WHERE id='R-1'")
        self.conn.execute(
            "UPDATE tasks SET status='cancelled', closed_at=25000 WHERE id='T-1'")
        self.conn.commit()
        svc.reconcile(limit=10)
        recovery = [r for r in self.outbox().list_all()
                    if r["kind"] == KIND_RECOVERY][0]
        recovery_id = recovery["item_id"]
        self.assertEqual(recovery["status"], STATUS_PENDING)
        # 底层源“复活”→ 恢复 claim 重验底层不再 resolved → 不发恢复邮件。
        self.conn.execute(
            "UPDATE responsibilities SET closed_at=NULL, state='waiting_human' "
            "WHERE id='R-1'")
        self.conn.execute(
            "UPDATE tasks SET status='draft', closed_at=NULL WHERE id='T-1'")
        self.conn.commit()
        res = mail.dispatch_once(item_id=recovery_id, owner="sidecar")
        self.assertNotEqual(res["result"], "accepted")
        self.assertEqual(len(self.transport.sent), 1)  # 只有首封已发，恢复未发

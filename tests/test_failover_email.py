#!/usr/bin/env python3
"""failover 故障知会桥测试（N01/N02/N07/N10/N11 + source 校验 + 发送委托）。

事件入队前必须把 episode 持久化到 FailoverLedger（source validator 在 outbox
事务内按台账重验归属，等价生产路径：fault_recovery.observe_failure 先登记）。
"""

from __future__ import annotations

import shutil
import tempfile
import unittest

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fakes import FakeClock

from oc_orch.adapters.sqlite_db import connect, init
from oc_orch.adapters.failover_ledger import FailoverLedger
from oc_orch.adapters.email_outbox import EmailOutbox
from oc_orch.application.operator_email import OperatorEmailService
from oc_orch.application.failover_email import FailoverEmailService
from oc_orch.domain.operator_email import (
    EmailPolicy,
    Recipient,
    Sender,
    STATUS_PENDING,
    STATUS_PENDING_AUTH,
)
from oc_orch.domain import failover as fo
from oc_orch.domain import failover_notify as notify


class FakeTransport:
    """最小 MailTransport：记录 send；可注入 queued 真/假。"""

    def __init__(self, queued=True):
        self.queued = queued
        self.sent = []
        self.searched = []

    def send(self, **kw):
        self.sent.append(kw)
        return {"queued": bool(self.queued), "external_ref": "msg-1"}

    def search_receipt(self, **kw):
        self.searched.append(kw)
        return []


def _policy(project="p1"):
    return EmailPolicy(
        enabled=True,
        sender=Sender(ref="agently-primary", identity="vicatst480@agent.qq.com"),
        recipients=(Recipient(ref="operator-primary", address="vicat47@qq.com"),),
        projects=frozenset({project}),
        event_kinds=frozenset({"first", "update", "escalation", "recovery"}),
        unattended=True,
        receipt_queries_authorized=True,
        max_attempts=3,
        receipt_check_limit=3,
        stale_claim_after_ms=300_000,
    )


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="oc-orch-femail-")
        init(self._tmp)
        self.clock = FakeClock(wall_ms=1_000_000)
        self.transport = FakeTransport()

    def tearDown(self):
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _svc(self, project="p1", policy=None):
        self.conn = connect(self._tmp)
        failover = FailoverLedger(self.conn, clock=self.clock.now_ms)
        outbox = EmailOutbox(
            self.conn, self.clock,
            source_validator=failover.make_source_validator(),
            require_source_binding=True)
        email = OperatorEmailService(outbox, self.transport, self.clock,
                                     policy or _policy(project))
        return FailoverEmailService(email, self.clock), failover

    def _seed(self, failover, *, status="open", project="p1",
              route="deepseek/deepseek-flash", category="quota"):
        episode_id = "EP-test1"
        now = self.clock.now_ms()
        rec = {
            "episode_id": episode_id, "project": project, "task_id": "T-1",
            "session_id": "ses_w1", "route": route, "category": category,
            "status": status, "switches_used": 0, "attempts_used": 1,
            "old_attempt_id": "A-1", "created_at": 1_000_000,
            "updated_at": now,
            "metadata": {"target": {"providerID": "yfzs-newapi",
                                    "id": "DeepSeek-AI/DeepSeek-V4-Flash"}},
        }
        existing = failover.get_record("episode", episode_id)
        if existing is not None:
            out = failover.upsert_record(
                "episode", episode_id, dict(rec, kind="episode",
                                             status=status, updated_at=now),
                expected_revision=existing["revision"])
        else:
            out = failover.upsert_record("episode", episode_id, rec,
                                         expected_revision=None)
        rec["kind"] = "episode"
        rec["revision"] = out["revision"]
        rec["updated_at"] = now
        return rec
        return rec


class TestEnqueue(Base):
    def test_first_event_enqueued_pending(self):
        svc, failover = self._svc()
        try:
            ep = self._seed(failover, status=fo.EPISODE_OPEN)
            out = svc.enqueue_episode_event(ep)
            self.assertEqual(out["result"], "enqueued")
            self.assertEqual(out["kind"], notify.KIND_FIRST)
            self.assertEqual(out["incident_type"], notify.EVENT_PROVIDER_FAILOVER)
            self.assertEqual(len(svc.list_items()), 1)
            item = svc.list_items()[0]
            self.assertEqual(item["status"], STATUS_PENDING)
            self.assertIn("deepseek/deepseek-flash", item["body"])
        finally:
            self.conn.close()

    def test_duplicate_revision_no_new_event(self):
        svc, failover = self._svc()
        try:
            ep = self._seed(failover)
            svc.enqueue_episode_event(ep)
            out = svc.enqueue_episode_event(ep)
            self.assertEqual(out["result"], "no-new-event")
            self.assertEqual(len(svc.list_items()), 1)
        finally:
            self.conn.close()

    def test_revision_bump_emits_update(self):
        svc, failover = self._svc()
        try:
            svc.enqueue_episode_event(self._seed(failover))
            ep2 = self._seed(failover, status=fo.EPISODE_SWITCH_CONFIRMED)
            out = svc.enqueue_episode_event(ep2)
            self.assertEqual(out["result"], "enqueued")
            self.assertEqual(out["kind"], notify.KIND_UPDATE)
            self.assertEqual(len(svc.list_items()), 2)
        finally:
            self.conn.close()

    def test_quick_resume_still_sends_first(self):
        # N07：快速恢复（未首发即 resumed 但 rows=[]）仍发首封摘要。
        svc, failover = self._svc()
        try:
            ep = self._seed(failover, status=fo.EPISODE_RESUMED)
            out = svc.enqueue_episode_event(ep)
            self.assertEqual(out["result"], "enqueued")
            self.assertEqual(out["kind"], notify.KIND_FIRST)
            text = svc.list_items()[0]["subject"] + svc.list_items()[0]["body"]
            self.assertIn("已切换并恢复执行", text)
        finally:
            self.conn.close()

    def test_blocked_manual_is_provider_fault(self):
        svc, failover = self._svc()
        try:
            ep = self._seed(failover, status=fo.EPISODE_BLOCKED_MANUAL)
            out = svc.enqueue_episode_event(ep)
            self.assertEqual(out["incident_type"], notify.EVENT_PROVIDER_FAULT)
            self.assertEqual(out["kind"], notify.KIND_FIRST)
        finally:
            self.conn.close()

    def test_out_of_scope_is_pending_auth_visible(self):
        # 项目不在授权范围：仍然登记可见 pending-auth（不发送、不丢失事实）。
        svc, failover = self._svc(project="p1")
        try:
            ep = self._seed(failover, project="other")
            out = svc.enqueue_episode_event(ep)
            self.assertEqual(out["result"], "enqueued")
            self.assertEqual(svc.list_items()[0]["status"], STATUS_PENDING_AUTH)
        finally:
            self.conn.close()

    def test_missing_episode_source_recheck_fails(self):
        svc, failover = self._svc()
        try:
            ep = self._seed(failover)
            ep["episode_id"] = "EP-missing"
            ep["metadata"]["target"] = {}
            out = svc.enqueue_episode_event(ep)
            self.assertEqual(out["result"], "source-recheck-failed")
            self.assertEqual(len(svc.list_items()), 0)
        finally:
            self.conn.close()


class TestDispatchDelegate(Base):
    def test_dispatch_sends_via_reused_logic(self):
        svc, failover = self._svc()
        try:
            ep = self._seed(failover, status=fo.EPISODE_OPEN)
            out = svc.enqueue_episode_event(ep)
            item_id = out["item_id"]
            self.clock.advance(seconds=1)
            result = svc.dispatch_once(owner="email-executor", item_id=item_id)
            self.assertEqual(result["result"], "accepted")
            self.assertEqual(len(self.transport.sent), 1)
            self.assertIn("vicat47@qq.com", self.transport.sent[0]["to"])
        finally:
            self.conn.close()


class TestValidator(unittest.TestCase):
    def test_binding_recheck(self):
        tmp = tempfile.mkdtemp(prefix="oc-orch-fv-")
        try:
            conn = init(tmp)
            clock = FakeClock(wall_ms=1)
            failover = FailoverLedger(conn, clock=clock.now_ms)
            ep = {
                "episode_id": "EP-v1", "project": "p", "task_id": "T-1",
                "session_id": "ses_w1", "route": "deepseek/deepseek-flash",
                "category": "quota", "status": "open", "switches_used": 0,
                "attempts_used": 1, "old_attempt_id": "A-1",
                "created_at": 1, "updated_at": 2, "metadata": {}}
            failover.upsert_record("episode", "EP-v1", ep, expected_revision=None)
            validate = failover.make_source_validator()
            ok_binding = {
                "source_kind": "failover-episode", "source_id": "EP-v1",
                "source_condition_key": "failover:EP-v1", "source_version": "v1",
                "source_state": "open",
                "source_incident_type": "provider-failover",
                "source_project": "p", "source_task_id": "T-1",
                "source_session": "ses_w1", "source_model": None}
            v1 = validate(conn, {"source_binding": ok_binding, "project": "p",
                                 "incident_type": "provider-failover"})
            self.assertTrue(v1["ok"])
            v2 = validate(conn, {"source_binding": dict(
                ok_binding, source_project="other")})
            self.assertFalse(v2["ok"])
            v3 = validate(conn, {"source_binding": dict(
                ok_binding, source_id="EP-nope")})
            self.assertFalse(v3["ok"])
            conn.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
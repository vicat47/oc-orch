#!/usr/bin/env python3
"""failover 故障知会桥测试（N01/N02/N07/N10/N11 + source 校验 + 发送委托）。

事件入队前必须把 episode 持久化到 FailoverLedger（source validator 在 outbox
事务内按台账重验归属，等价生产路径：fault_recovery.observe_failure 先登记）。
"""

from __future__ import annotations

import shutil
import hashlib
import tempfile
import threading
import unittest

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fakes import FakeClock

from oc_orch.adapters.sqlite_db import connect, init
from oc_orch.adapters.writer_guard import install_writer_guard
from oc_orch.adapters.failover_guard import install_failover_guard
from oc_orch import composition
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


class TestConfiguredFailoverEmail(unittest.TestCase):
    def test_missing_email_policy_keeps_failover_email_disabled(self):
        with tempfile.TemporaryDirectory(prefix="oc-orch-femail-config-") as home:
            conn = init(home)
            try:
                service = composition.make_configured_failover_email(
                    conn, home, clock=FakeClock(wall_ms=1_000_000))
            finally:
                conn.close()

        self.assertIsNone(service)


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
        conn = init(self._tmp)
        install_writer_guard(conn, actor="test")
        install_failover_guard(conn, actor="test")
        conn.close()
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

    def test_revision_bump_coalesces_unclaimed_first_event(self):
        svc, failover = self._svc()
        try:
            svc.enqueue_episode_event(self._seed(failover))
            ep2 = self._seed(failover, status=fo.EPISODE_SWITCH_CONFIRMED)
            out = svc.enqueue_episode_event(ep2)
            self.assertEqual(out["result"], "enqueued")
            self.assertEqual(out["kind"], notify.KIND_FIRST)
            self.assertTrue(out.get("coalesced"))
            self.assertEqual(len(svc.list_items()), 1)
            self.assertIn("切换已确认", svc.list_items()[0]["body"])
        finally:
            self.conn.close()

    def test_wal_claim_races_first_event_coalesce_and_freezes_claimed_content(self):
        svc, failover = self._svc()
        try:
            first = svc.enqueue_episode_event(self._seed(failover))
            item_id = first["item_id"]
            before = svc.list_items()[0]
            updated_episode = self._seed(
                failover, status=fo.EPISODE_SWITCH_CONFIRMED)
            binding = svc._binding(updated_episode,
                                   notify.EVENT_PROVIDER_FAILOVER, 2)
            subject = "updated after switch confirmation"
            body = "The same incident now has a confirmed fallback route."
            fields = {
                "condition_version": 2, "escalation_level": 0,
                "subject": subject, "body": body,
                "content_hash": hashlib.sha256(
                    f"{subject}\x00{body}".encode()).hexdigest(),
                "status": STATUS_PENDING,
                "last_observed_at": self.clock.now_ms(),
                "summary": {"episode_id": updated_episode["episode_id"],
                            "status": fo.EPISODE_SWITCH_CONFIRMED,
                            "condition_key": f"failover:{updated_episode['episode_id']}",
                            "version": 2, "delivery_block_reason": None},
            }
            barrier = threading.Barrier(3)
            results, errors = {}, []

            def coalesce():
                conn = connect(self._tmp)
                try:
                    outbox = EmailOutbox(conn, self.clock)
                    barrier.wait()
                    results["coalesce"] = outbox.coalesce_unclaimed(
                        item_id, expected_revision=before["revision"],
                        fields=fields, now=self.clock.now_ms(),
                        source_binding=binding)
                except Exception as exc:
                    errors.append(exc)
                finally:
                    conn.close()

            def claim():
                conn = connect(self._tmp)
                try:
                    outbox = EmailOutbox(conn, self.clock)
                    barrier.wait()
                    results["claim"] = outbox.claim(
                        item_id, owner="wal-sender", now=self.clock.now_ms())
                except Exception as exc:
                    errors.append(exc)
                finally:
                    conn.close()

            threads = [threading.Thread(target=coalesce),
                       threading.Thread(target=claim)]
            for thread in threads:
                thread.start()
            barrier.wait()
            for thread in threads:
                thread.join(timeout=10)

            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual(errors, [])
            self.assertTrue(results["claim"]["allowed"], results)
            claimed = results["claim"]["item"]
            persisted = svc.list_items()[0]
            self.assertEqual(persisted["status"], "claimed")
            self.assertEqual(persisted["possible_submitted_at"], self.clock.now_ms())
            self.assertEqual((persisted["subject"], persisted["body"]),
                             (claimed["subject"], claimed["body"]))
            self.assertIn(persisted["condition_version"], (1, 2))
            self.assertEqual(sum(row["kind"] == notify.KIND_FIRST
                                 for row in svc.list_items()), 1)

            # Even if a coalescer retries after observing the claim, the exact
            # subject/body frozen for the sender remain unchanged.
            c = connect(self._tmp)
            try:
                retry = EmailOutbox(c, self.clock).coalesce_unclaimed(
                    item_id, expected_revision=persisted["revision"],
                    fields=fields, now=self.clock.now_ms(),
                    source_binding=binding)
            finally:
                c.close()
            self.assertFalse(retry["coalesced"])
            after = svc.list_items()[0]
            self.assertEqual(after["subject"], claimed["subject"])
            self.assertEqual(after["body"], claimed["body"])
        finally:
            self.conn.close()

    def test_ordinary_manual_switch_has_no_failover_email_event(self):
        svc, failover = self._svc()
        try:
            ordinary_switch = {"status": "manual_switch", "project": "p1",
                               "episode_id": "ordinary-switch"}
            out = svc.enqueue_episode_event(ordinary_switch)
            self.assertEqual(out["result"], "no-event-type")
            self.assertEqual(svc.list_items(), [])
        finally:
            self.conn.close()

    def test_claimed_incident_keeps_content_and_update_budget_across_reopen(self):
        svc, failover = self._svc()
        first = svc.enqueue_episode_event(self._seed(failover))
        original_body = svc.list_items()[0]["body"]
        claim = svc.email._outbox.claim(
            first["item_id"], owner="sender-before-restart", now=self.clock.now_ms())
        self.assertTrue(claim["allowed"])
        self.conn.close()

        states = [fo.EPISODE_SWITCH_REQUESTED,
                  fo.EPISODE_SWITCH_CONFIRMED,
                  fo.EPISODE_CONTINUATION_REQUESTED,
                  fo.EPISODE_CONTINUATION_UNKNOWN]
        outcomes = []
        for index, state in enumerate(states):
            svc, failover = self._svc()
            episode = self._seed(failover, status=state)
            outcomes.append(svc.enqueue_episode_event(episode))
            if index < 3:
                self.assertEqual(outcomes[-1]["result"], "enqueued")
                self.assertEqual(outcomes[-1]["kind"], notify.KIND_UPDATE)
            else:
                self.assertEqual(outcomes[-1]["result"], "no-new-event")
                self.assertEqual(outcomes[-1]["reason"], "update-budget-exhausted")
            self.conn.close()

        svc, _ = self._svc()
        rows = svc.list_items()
        self.assertEqual(len(rows), 4)  # first + the configured three updates
        first_row = next(row for row in rows if row["kind"] == notify.KIND_FIRST)
        self.assertEqual(first_row["status"], "claimed")
        self.assertEqual(first_row["body"], original_body)
        self.assertIsNotNone(first_row["possible_submitted_at"])
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
            self.assertIn("人工处理", svc.list_items()[0]["body"])
            self.assertIn("下一动作", svc.list_items()[0]["body"])
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
            install_writer_guard(conn, actor="test")
            install_failover_guard(conn, actor="test")
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

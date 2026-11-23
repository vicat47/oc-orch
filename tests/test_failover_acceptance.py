"""Provider failover acceptance through the real SQLite composition and fake services.

This deliberately stops at the development/fake boundary: no OpenCode server or
mail transport is created. It exercises the shared dispatch, collect and verify
use cases around the failover state machine.
"""

from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oc_orch.adapters.failover_guard import install_failover_guard
from oc_orch.adapters.failover_ledger import FailoverLedger
from oc_orch.adapters.results import collect_attempt, verify_attempt
from oc_orch.adapters.sqlite_db import init
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.writer_guard import install_writer_guard
from oc_orch.composition import application, make_failover_email
from oc_orch.config import DEFAULTS
from oc_orch.domain.operator_email import EmailPolicy, Recipient, Sender
from oc_orch.runtime.supervisor import Supervisor
from tests.fakes import FakeClient, FakeClock, complete_execution


class FakeMailTransport:
    def __init__(self):
        self.sent = []

    def send(self, **kwargs):
        self.sent.append(kwargs)
        return {"queued": True, "external_ref": "fake-mail-accepted"}

    def search_receipt(self, **kwargs):
        return []


class ProviderFailoverEndToEnd(unittest.TestCase):
    def test_authorized_quota_failure_switches_same_session_collects_then_verifies(self):
        with tempfile.TemporaryDirectory(prefix="failover-e2e-") as home:
            clock = FakeClock(wall_ms=1_000_000)
            client = FakeClient()
            old_route = "deepseek/deepseek-flash"
            target_route = "yfzs-newapi/DeepSeek-AI/DeepSeek-V4-Flash"
            worker, coordinator, project = "ses-worker", "ses-coordinator", "failover-e2e"
            setup_conn = init(home)
            install_writer_guard(setup_conn, actor="acceptance")
            install_failover_guard(setup_conn, actor="acceptance")
            setup_conn.close()
            sup = Supervisor(home, cfg=DEFAULTS, client=client, clock=clock)
            self.addCleanup(sup.conn.close)
            conn = sup.conn
            ledger = sup.ledger
            ledger.claim_coordinator(project=project, session_id=coordinator,
                                     actor="acceptance", ttl=3600)
            client.set_session(coordinator, location_dir=home)
            client.set_session(worker, location_dir=home,
                               model={"providerID": "deepseek", "id": "deepseek-flash"})
            task = ledger.create_task(
                project=project, title="failover acceptance", objective="produce result.txt",
                acceptance=["result.txt"], write_scope=[home],
                coordinator_session=coordinator)

            failover = FailoverLedger(conn, clock=clock.now_ms)
            failover.set_policy(
                "project", project,
                {"enabled": True, "scope": "project", "source_routes": [old_route],
                 "fallback_routes": [target_route], "max_switches": 1,
                 "max_attempts": 3}, expected_revision=None)
            from oc_orch.composition import make_resource_authorization
            resource_auth = make_resource_authorization(ledger, clock)
            resource_auth.replace(
                "task", task["id"],
                resources=[{"model": target_route, "cost": "paid",
                            "evidence": "fake acceptance authorization",
                            "internal_calls_controlled": True}],
                expires_at=clock.now_ms() + 3_600_000, expected_revision=0,
                operation_id="resource-auth-failover-e2e", actor="acceptance",
                reason="explicit fake acceptance scope")

            orchestration = application(ledger, lambda: client, DEFAULTS, clock=clock)
            original = orchestration.dispatch(
                task["id"], directory=home, coordinator=coordinator, session=worker,
                model=old_route, scope=[home], deliverables=["result.txt"],
                deadline=900, stall_after=120, prompt="produce the result")
            old_attempt = ledger.get_attempt(original["attempt_id"])
            input_at = clock.now_ms()
            client.deliver_inbox(worker, at=input_at)
            ledger.record_execution_started(old_attempt["id"], started_at=input_at,
                                            event_id="e2e-old-start")
            clock.advance(seconds=1)
            complete_execution(ledger, old_attempt["id"], outcome="failed",
                               input_at=input_at, ended_at=clock.now_ms())
            client.set_session(worker, outcome="failed",
                               time_={"busy": input_at, "idle": clock.now_ms(),
                                      "updated": clock.now_ms()},
                               location_dir=home,
                               model={"providerID": "deepseek", "id": "deepseek-flash"})

            # Exercise the same post-terminal integration used after Supervisor
            # accepts an SSE/REST terminal fact: release proof, task notification,
            # then provider failure registration from the structured 402.
            sup._after_terminal(
                ledger.get_attempt(old_attempt["id"]), "failed", reason="provider-402",
                terminal_error={"status": 402, "message": "Insufficient Balance"})
            recovery = sup._fault_recovery_service()
            episodes = recovery.failover.list_records("episode", limit=10)["records"]
            self.assertEqual(len(episodes), 1)
            episode_id = episodes[0]["episode_id"]
            recovery.tick(episode_id=episode_id)
            self.assertEqual(len(client.switch_calls), 1)
            self.assertEqual(client.switch_calls[0]["session_id"], worker)
            self.assertEqual(client.session_model(worker),
                             {"providerID": "yfzs-newapi",
                              "id": "DeepSeek-AI/DeepSeek-V4-Flash"})

            clock.advance(seconds=60)
            recovery.tick(episode_id=episode_id)  # confirm the actual fake session model
            recovery.tick(episode_id=episode_id)  # shared dispatch creates one successor
            episode = failover.get_record("episode", episode_id)
            successor_id = episode["metadata"]["successor_attempt_id"]
            message_id = episode["metadata"]["successor_message_id"]
            successor = ledger.get_attempt(successor_id)
            self.assertEqual(successor["session_id"], worker)
            self.assertEqual(successor["dispatch_msg_id"], message_id)
            self.assertEqual(successor["model"], target_route)
            self.assertEqual(successor["metadata"]["write_scope"], [home])
            self.assertIn("result.txt", [row["target"] for row in
                                          ledger.deliverables(successor_id)])
            self.assertEqual(successor["deadline_at"], old_attempt["deadline_at"])
            self.assertEqual(successor["stall_after"], old_attempt["stall_after"])
            self.assertIn(old_attempt["id"], client.prompt_calls[-1]["text"])
            self.assertEqual(successor["metadata"]["provider_failover"], {
                "episode_id": episode_id,
                "operation_id": episode["metadata"]["operation_id"],
                "old_attempt_id": old_attempt["id"],
                "failed_route": old_route,
            })
            self.assertEqual(len(client.switch_calls), 1)
            self.assertEqual(len(client.prompt_calls), 2)
            self.assertEqual(client.prompt_calls[-1]["session_id"], worker)
            self.assertEqual(client.prompt_calls[-1]["message_id"], message_id)

            successor_started = clock.now_ms()
            client.deliver_inbox(worker, at=successor_started)
            ledger.record_execution_started(successor_id, started_at=successor_started,
                                            event_id="e2e-successor-start")
            client.append_message(worker, {
                "id": "e2e-successor-answer", "type": "assistant", "finish": "stop",
                "time": {"created": successor_started + 1,
                         "completed": successor_started + 5},
                "content": [{"type": "text", "text": "result produced"}]})
            client.add_idle(worker, outcome="succeeded", at=successor_started + 10)
            client.set_session(worker, outcome="succeeded",
                               time_={"busy": successor_started,
                                      "idle": successor_started + 10,
                                      "updated": successor_started + 10},
                               location_dir=home,
                               model={"providerID": "yfzs-newapi",
                                      "id": "DeepSeek-AI/DeepSeek-V4-Flash"})
            Path(home, "result.txt").write_text("fake accepted result", encoding="utf-8")
            complete_execution(ledger, successor_id, input_at=successor_started,
                               ended_at=successor_started + 10)
            collect_attempt(ledger, client, successor_id, DEFAULTS)

            self.assertEqual(ledger.get_attempt(successor_id)["status"], "result_collected")
            self.assertEqual(ledger.get_task(task["id"])["status"], "result_received")
            self.assertNotEqual(ledger.get_task(task["id"])["status"], "accepted")
            recovery.tick(episode_id=episode_id)
            self.assertEqual(failover.get_record("episode", episode_id)["status"], "resumed")
            self.assertTrue(failover.is_route_blocked(old_route)["blocked"])

            # Recovery is a separate business acceptance decision.
            accepted = verify_attempt(ledger, successor_id, result="pass",
                                      evidence=["fake-e2e-result.txt"])
            self.assertTrue(accepted["ok"])
            self.assertEqual(ledger.get_task(task["id"])["status"], "accepted")

            # An episode that recovered before mail dispatch still gets one valid summary.
            transport = FakeMailTransport()
            mail_policy = EmailPolicy(
                enabled=True,
                sender=Sender(ref="fake-sender", identity="test@example.invalid"),
                recipients=(Recipient(ref="operator-primary",
                                      address="operator@example.invalid"),),
                projects=frozenset({project}),
                event_kinds=frozenset({"first", "update", "escalation", "recovery"}),
                unattended=True, receipt_queries_authorized=True)
            email = make_failover_email(conn, clock=clock, policy=mail_policy,
                                       transport=transport)
            enqueued = email.enqueue_episode_event(
                failover.get_record("episode", episode_id))
            self.assertEqual(enqueued["result"], "enqueued")
            sent = email.dispatch_once(owner="fake-mail-executor",
                                       item_id=enqueued["item_id"])
            self.assertEqual(sent["result"], "accepted")
            self.assertEqual(len(transport.sent), 1)
            self.assertIn(old_route, transport.sent[0]["body"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

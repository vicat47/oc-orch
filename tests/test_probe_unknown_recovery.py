"""原 probe 超时后的补证：不重投、预算跨重启、晚到成功与 CAS 守卫。"""

from __future__ import annotations

import json
import sqlite3
import unittest
from unittest.mock import patch

from oc_orch.adapters.sqlite_db import init, meta_get, meta_set
from oc_orch.adapters.target_gate import TargetGate, TargetGateStateError
from oc_orch.client import TransportError
from oc_orch.config import DEFAULTS
from oc_orch.supervisor import Supervisor
from tests.test_target_gate import GateBase, _open_wal, quota
from tests.test_target_gate_runtime import TargetGateRuntimeBase


class ProbeUnknownRecoveryTests(TargetGateRuntimeBase):
    def start_probe(self):
        self.register_coordinator()
        self.open_gate(retry_after_ms=1)
        self.clock.advance(wall=2)
        self.notice = self.enqueue("original-probe")
        self.sup.dispatch_notifications()
        self.assertEqual(len(self.client.prompt_calls), 1)
        self.probe_id = self.client.prompt_calls[0]["message_id"]
        self.clock.advance(wall=1)
        self.client.deliver_inbox("ses_coord", at=self.clock.now_ms())
        return self.probe_id

    def finish(self, *, late=True):
        self.clock.advance(seconds=11 * 60 if late else 1)
        self.idle_at = self.clock.now_ms()
        self.idle = self.client.add_idle(
            "ses_coord", outcome="succeeded", at=self.idle_at)
        self.client.set_session(
            "ses_coord", outcome="succeeded", time_={"idle": self.idle_at})

    def test_late_success_clears_original_gate_without_ack_or_reexecution(self):
        """C17/C18：半开过期后唯一成功可补证，不重投、不验收。"""
        self.start_probe()
        self.finish()
        self.assertEqual(self.gate.get("ses_coord")["state"], "probe-unknown")

        self.sup.observe_coordinators({})

        self.assertIsNone(self.gate.get("ses_coord"))
        self.assertEqual(len(self.client.prompt_calls), 1)
        row = self.ledger.get_notification(self.notice["id"])
        self.assertEqual(row["message_id"], self.probe_id)
        self.assertEqual(row["state"], "sent")
        self.assertIsNone(row["acked_at"])
        audits = self.sup.conn.execute(
            "SELECT detail FROM audit WHERE action='target-gate.clear-probe-success' "
            "AND entity_id='ses_coord'").fetchall()
        self.assertEqual(len(audits), 1)
        self.assertEqual(json.loads(audits[0]["detail"])["probe_message_id"],
                         self.probe_id)

    def test_same_terminal_stamp_rechecks_after_incomplete_query(self):
        """C18：首轮查询失败不能因 coord_outcome 已记 seen 永远失去补证。"""
        self.start_probe()
        self.finish()
        self.client.fail("message_page", TransportError("/message", "lost"))
        with patch.object(self.sup, "_window", wraps=self.sup._window) as query:
            self.sup.observe_coordinators({})
            self.assertIsNotNone(self.gate.get("ses_coord"))
            self.assertEqual(query.call_count, 1)
            self.sup.observe_coordinators({})
            self.assertEqual(query.call_count, 1, "退避内不重复网络扫描")
            self.clock.advance(seconds=31)
            self.sup.observe_coordinators({})
            self.assertEqual(query.call_count, 2)
        self.assertIsNone(self.gate.get("ses_coord"))
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_recheck_budget_persists_and_new_terminal_fact_is_not_a_reset(self):
        """C20：相同事实只补证有限次；新结束事实允许一次补证而非重置预算。"""
        self.start_probe()
        self.finish()
        with patch.object(Supervisor, "_window", return_value=([], {"complete": False})) as query:
            self.sup.observe_coordinators({})
            first = self.gate.get("ses_coord")["probe_observation"]
            for _ in range(first["max_checks"] - 1):
                self.clock.advance(seconds=31)
                self.sup.observe_coordinators({})
            self.assertEqual(query.call_count, first["max_checks"])
            self.clock.advance(seconds=31)
            other = Supervisor(str(self.home), cfg=dict(DEFAULTS),
                               client=self.client, clock=self.clock)
            self.addCleanup(other.conn.close)
            other.observe_coordinators({})
            self.assertEqual(query.call_count, first["max_checks"])
            persisted = other.target_gate.get("ses_coord")["probe_observation"]
            self.assertEqual(persisted["checks"], first["max_checks"])
            self.assertEqual(persisted["deadline_at"], first["deadline_at"])

            self.clock.advance(seconds=1)
            self.client.set_session("ses_coord", outcome="succeeded",
                                    time_={"idle": self.clock.now_ms()})
            other.observe_coordinators({})
            self.assertEqual(query.call_count, first["max_checks"] + 1)
            other.observe_coordinators({})
            self.assertEqual(query.call_count, first["max_checks"] + 1)
        self.assertEqual(len(self.client.prompt_calls), 1)
        alerts = [a for a in self.ledger.list_alerts(state="new", limit=100)
                  if a["kind"] == "target_gate_probe_unknown"]
        self.assertEqual(len(alerts), 1)

    def test_multiple_idle_messages_do_not_prove_a_unique_probe_execution(self):
        """C19：同一窗口早已有失败 idle，不能挑最后一个 succeeded 清门。"""
        self.start_probe()
        self.clock.advance(seconds=1)
        self.client.add_idle("ses_coord", outcome="failed", at=self.clock.now_ms())
        self.finish(late=False)
        self.sup.observe_coordinators({})
        self.assertIsNotNone(self.gate.get("ses_coord"))

    def test_later_user_input_keeps_probe_unknown(self):
        self.start_probe()
        self.clock.advance(seconds=1)
        self.client.append_message("ses_coord", {
            "id": "msg_external", "type": "user", "text": "status?",
            "time": {"created": self.clock.now_ms()}})
        self.finish()
        self.sup.observe_coordinators({})
        self.assertEqual(self.gate.get("ses_coord")["probe_message_id"], self.probe_id)
        self.assertEqual(len(self.client.prompt_calls), 1)

    def test_synthetic_input_is_not_silently_attributed_to_probe(self):
        self.start_probe()
        self.clock.advance(seconds=1)
        self.client.append_message("ses_coord", {
            "id": "msg_synthetic", "type": "synthetic", "text": "later input",
            "time": {"created": self.clock.now_ms()}})
        self.finish(late=False)
        self.sup.observe_coordinators({})
        self.assertIsNotNone(self.gate.get("ses_coord"))

    def test_gate_change_during_network_query_requires_a_fresh_snapshot(self):
        """C19：新错误发生于查询窗口，旧快照即使匹配 message 也不得清门。"""
        self.start_probe()
        self.finish(late=False)
        original = self.sup._window

        def change_gate(*args, **kwargs):
            out = original(*args, **kwargs)
            self.gate.observe_error(
                "ses_coord", quota(), evidence_key="late-earlier-retry",
                observed_at=self.clock.now_ms(), origin_at=self.idle_at - 1)
            return out

        with patch.object(self.sup, "_window", side_effect=change_gate):
            self.sup.observe_coordinators({})
        self.assertIsNotNone(self.gate.get("ses_coord"))
        self.clock.advance(seconds=31)
        self.sup.observe_coordinators({})
        self.assertIsNone(self.gate.get("ses_coord"), "新快照允许核验同一完整事实")

    def test_newer_provider_failure_cannot_be_erased_by_older_success(self):
        self.start_probe()
        self.finish()
        self.clock.advance(seconds=1)
        self.gate.observe_error(
            "ses_coord", quota(), evidence_key="newer-failure",
            observed_at=self.clock.now_ms(), origin_at=self.clock.now_ms())
        self.sup.observe_coordinators({})
        self.assertEqual(self.gate.get("ses_coord")["last_error_key"], "newer-failure")

    def test_manual_pause_during_network_query_is_preserved(self):
        self.start_probe()
        self.finish(late=False)
        original = self.sup._window

        def pause(*args, **kwargs):
            out = original(*args, **kwargs)
            self.gate.block_manual("ses_coord", reason="coordinator-paused")
            return out

        with patch.object(self.sup, "_window", side_effect=pause):
            self.sup.observe_coordinators({})
        self.assertEqual(self.gate.get("ses_coord")["reason"], "coordinator-paused")

    def test_user_at_same_timestamp_before_probe_in_sort_order_is_ambiguous(self):
        self.start_probe()
        message = self.client.get_message("ses_coord", self.probe_id)
        self.client.append_message("ses_coord", {
            "id": "msg_000_other", "type": "user", "text": "other input",
            "time": dict(message["time"])})
        self.finish(late=False)
        self.sup.observe_coordinators({})
        self.assertIsNotNone(self.gate.get("ses_coord"))

    def test_pending_input_or_unrelated_idle_cannot_replace_original_identity(self):
        self.start_probe()
        self.finish()
        self.client._messages_store["ses_coord"] = [self.idle]
        self.sup.observe_coordinators({})
        snap = self.gate.get("ses_coord")
        self.assertEqual(snap["probe_message_id"], self.probe_id)
        self.assertEqual(snap["probe_count"], 1)
        self.assertEqual(len(self.client.prompt_calls), 1)


class ProbeRecoveryGateTests(GateBase):
    def probe(self):
        self.gate.observe_error("ses_a", quota(retry_after_ms=1),
                                evidence_key="initial", observed_at=self.t0)
        self.clock.advance(wall=2)
        return self.gate.claim_probe("ses_a", owner="sup", message_id="msg_probe")["state"]

    def test_success_rejects_stale_expected_revision(self):
        snap = self.probe()
        self.clock.advance(seconds=1)
        out = self.gate.record_success(
            "ses_a", message_id="msg_probe", observed_at=self.t0,
            evidence_key="success", expected_revision=snap["revision"] - 1)
        self.assertFalse(out["cleared"])
        self.assertEqual(out["reason"], "revision-changed")
        self.assertEqual(self.gate.get("ses_a"), snap)

    def test_success_rejects_newer_error_even_without_a_revision_argument(self):
        snap = self.probe()
        self.clock.advance(seconds=2)
        self.gate.observe_error("ses_a", quota(), evidence_key="new-error",
                                observed_at=self.t0, origin_at=self.t0)
        out = self.gate.record_success(
            "ses_a", message_id="msg_probe", observed_at=snap["probe_started_at"] + 1,
            evidence_key="old-success")
        self.assertFalse(out["cleared"])
        self.assertEqual(out["reason"], "newer-error")

    def test_observation_claim_is_atomic_across_connections(self):
        snap = self.probe()
        self.clock.advance(seconds=1)
        conn2 = _open_wal(self.home)
        self.addCleanup(conn2.close)
        other = TargetGate(conn2, self.clock)
        first = self.gate.claim_probe_observation(
            "ses_a", message_id="msg_probe", expected_revision=snap["revision"],
            idle_at=self.t0)
        second = other.claim_probe_observation(
            "ses_a", message_id="msg_probe", expected_revision=snap["revision"],
            idle_at=self.t0)
        self.assertTrue(first["allowed"])
        self.assertFalse(second["allowed"])
        self.assertEqual(other.get("ses_a")["probe_observation"]["checks"], 1)

    def test_observation_deadline_does_not_reset_on_restart(self):
        snap = self.probe()
        self.clock.advance(seconds=1)
        first = self.gate.claim_probe_observation(
            "ses_a", message_id="msg_probe", expected_revision=snap["revision"],
            idle_at=self.t0)
        observation = first["gate"]["probe_observation"]
        self.clock.advance(wall=observation["deadline_at"] - self.t0 + 1)
        conn2 = init(self.home)
        self.addCleanup(conn2.close)
        other = TargetGate(conn2, self.clock)
        current = other.get("ses_a")
        denied = other.claim_probe_observation(
            "ses_a", message_id="msg_probe", expected_revision=current["revision"],
            idle_at=observation["last_idle_at"])
        self.assertFalse(denied["allowed"])
        self.assertEqual(denied["reason"], "observation-exhausted")
        self.assertEqual(other.get("ses_a")["probe_observation"], observation)

    def test_invalid_observation_state_fails_closed(self):
        snap = self.probe()
        self.clock.advance(seconds=1)
        self.gate.claim_probe_observation(
            "ses_a", message_id="msg_probe", expected_revision=snap["revision"],
            idle_at=self.t0)
        raw = json.loads(meta_get(self.conn, "target-gate:ses_a"))
        for field, value in (("checks", True), ("schema", 2), ("schema", True),
                             ("started_at", float(raw["probe_started_at"])),
                             ("message_id", "different"), ("max_checks", 0)):
            with self.subTest(field=field):
                bad = dict(raw)
                bad["probe_observation"] = dict(raw["probe_observation"], **{field: value})
                encoded = json.dumps(bad)
                meta_set(self.conn, "target-gate:ses_a", encoded)
                with self.assertRaises(TargetGateStateError):
                    self.gate.get("ses_a")
                self.assertEqual(meta_get(self.conn, "target-gate:ses_a"), encoded)
        meta_set(self.conn, "target-gate:ses_a", json.dumps(raw))

    def test_bad_revision_and_future_idle_do_not_mutate_observation(self):
        snap = self.probe()
        before = meta_get(self.conn, "target-gate:ses_a")
        for revision in (True, 0, -1, "2", 2.0):
            with self.subTest(revision=revision):
                out = self.gate.claim_probe_observation(
                    "ses_a", message_id="msg_probe", expected_revision=revision,
                    idle_at=self.t0)
                self.assertFalse(out["allowed"])
                self.assertEqual(meta_get(self.conn, "target-gate:ses_a"), before)
        out = self.gate.claim_probe_observation(
            "ses_a", message_id="msg_probe", expected_revision=snap["revision"],
            idle_at=self.t0 + 1)
        self.assertFalse(out["allowed"])
        self.assertEqual(meta_get(self.conn, "target-gate:ses_a"), before)

    def test_success_and_audit_are_rolled_back_together(self):
        self.probe()
        self.clock.advance(seconds=1)
        before = meta_get(self.conn, "target-gate:ses_a")
        self.conn.execute(
            "CREATE TRIGGER reject_clear_audit BEFORE INSERT ON audit "
            "WHEN NEW.action='target-gate.clear-probe-success' "
            "BEGIN SELECT RAISE(ABORT, 'audit-injection'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.gate.record_success(
                "ses_a", message_id="msg_probe", observed_at=self.t0,
                evidence_key="success")
        self.assertEqual(meta_get(self.conn, "target-gate:ses_a"), before)
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM audit WHERE action='target-gate.clear-probe-success'"
        ).fetchone()[0], 0)

    def test_old_gate_without_observation_is_readable_and_sql_schema_unchanged(self):
        snap = self.probe()
        raw = json.loads(meta_get(self.conn, "target-gate:ses_a"))
        self.assertNotIn("probe_observation", raw)
        schema = meta_get(self.conn, "schema_version")
        tables = self.conn.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        self.clock.advance(seconds=1)
        claim = self.gate.claim_probe_observation(
            "ses_a", message_id="msg_probe", expected_revision=snap["revision"],
            idle_at=self.t0)
        self.assertTrue(claim["allowed"])
        self.assertEqual(meta_get(self.conn, "schema_version"), schema)
        self.assertEqual([tuple(x) for x in self.conn.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()], [tuple(x) for x in tables])

    def test_old_idle_cannot_bypass_budget_after_a_newer_observation(self):
        snap = self.probe()
        self.clock.advance(seconds=2)
        claim = self.gate.claim_probe_observation(
            "ses_a", message_id="msg_probe", expected_revision=snap["revision"],
            idle_at=self.t0)
        before = meta_get(self.conn, "target-gate:ses_a")
        out = self.gate.claim_probe_observation(
            "ses_a", message_id="msg_probe", expected_revision=claim["gate"]["revision"],
            idle_at=self.t0 - 1)
        self.assertFalse(out["allowed"])
        self.assertEqual(out["reason"], "stale-observation")
        self.assertEqual(meta_get(self.conn, "target-gate:ses_a"), before)

    def test_consistent_backup_preserves_optional_observation_and_budget(self):
        """增量字段不依赖DDL：一致副本保留原ID/预算，恢复不重新获得补查次数。"""
        snap = self.probe()
        self.clock.advance(seconds=1)
        claim = self.gate.claim_probe_observation(
            "ses_a", message_id="msg_probe", expected_revision=snap["revision"],
            idle_at=self.t0)
        copied = sqlite3.connect(self.home + "/probe-backup.db", isolation_level=None)
        self.addCleanup(copied.close)
        copied.row_factory = sqlite3.Row
        self.conn.backup(copied)
        self.assertEqual(copied.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        restored = TargetGate(copied, self.clock)
        self.assertEqual(restored.get("ses_a"), claim["gate"])
        result = restored.claim_probe_observation(
            "ses_a", message_id="msg_probe", expected_revision=claim["gate"]["revision"],
            idle_at=self.t0)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "observation-backoff")
        self.assertEqual(restored.get("ses_a")["probe_observation"]["checks"], 1)


if __name__ == "__main__":
    unittest.main()

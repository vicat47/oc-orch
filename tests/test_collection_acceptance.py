"""完成协议独立验收：真实 collect 交错、诊断、分页和恢复责任。"""
import copy
import json
from pathlib import Path
from unittest.mock import patch

from tests.test_provenance_remediation import RemediationBase, CFG, _assistant
from tests.test_collection_remediation import RemediationBase as LegacyBase
from tests.fakes import complete_execution, FakeClock
from oc_orch.adapters import results
from oc_orch.db import init
from oc_orch.ledger import Ledger, LedgerError
from oc_orch.supervisor import Supervisor


class CollectionAcceptanceTests(RemediationBase):
    def collect(self, att, **kwargs):
        return results.collect_attempt(self.ledger, self.client, att["id"], CFG, **kwargs)

    def rows(self, att):
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM notifications WHERE attempt_id=? ORDER BY id", (att["id"],))]

    def test_active_partial_and_post_result_partial_keep_authority(self):
        task, att = self._attempt(deliverables=[])
        before = self.ledger.get_attempt(att["id"])
        partial = self.collect(att, partial=True)
        self.assertTrue(partial["partial"])
        self.assertTrue(Path(partial["diagnostic_path"]).is_file())
        self.assertIsNone(partial["result_path"])
        self.assertEqual(self.ledger.get_attempt(att["id"]), before)
        self.assertEqual(self.rows(att), [])
        self._messages("ses_w", ["完成"], "msg_in", 1000)
        complete_execution(self.ledger, att["id"], input_at=1000, ended_at=2000)
        self.collect(att)
        before, notifications = self.ledger.get_attempt(att["id"]), self.rows(att)
        self.collect(att, partial=True)
        self.assertEqual(self.ledger.get_attempt(att["id"]), before)
        self.assertEqual(self.rows(att), notifications)

    def test_tools_only_are_not_substantive_but_new_file_is(self):
        _, att, out = self._collected(deliverables=(), texts=())
        self.client.append_message("ses_w", {
            "id": "tool-progress", "type": "assistant", "time": {"created": 1000040},
            "content": [{"type": "tool", "name": "bash", "executed": True,
                         "state": {"status": "completed"}}]})
        out = self.collect(att)
        self.assertGreater(out["assistant_message_count"], 0)
        self.assertFalse(out["effective"])
        self.assertIsNone(self.ledger.get_attempt(att["id"])["result_path"])
        self.assertFalse(any(n["kind"] == "result_ready" for n in self.rows(att)))
        self.ledger.add_deliverables(att["id"], ["new.txt"])
        self._write("new.txt", "新产物")
        out = self.collect(att)
        self.assertTrue(out["effective"])
        self.assertTrue(out["ready_for_verification"])

    def test_message_error_keeps_previous_version_and_deliverables(self):
        _, att, out = self._collected(final_contents={"out.txt": "new"})
        before, rows = self.ledger.get_attempt(att["id"]), self.ledger.deliverables(att["id"])
        notices = self.rows(att)
        self.client.fail("message_page", RuntimeError("read unavailable"))
        with self.assertRaises(LedgerError) as cm:
            self.collect(att)
        self.assertEqual(cm.exception.code, "COLLECT_FAILED")
        self.assertEqual(self.ledger.get_attempt(att["id"]), before)
        self.assertEqual(self.ledger.deliverables(att["id"]), rows)
        self.assertEqual(self.rows(att), notices)

    def test_incomplete_file_scan_keeps_previous_version_and_deliverables(self):
        _, att, _ = self._collected(final_contents={"out.txt": "original content"})
        before = self.ledger.get_attempt(att["id"])
        rows, notices = self.ledger.deliverables(att["id"]), self.rows(att)
        config = {**CFG, "deliverable_scan_max_bytes": 1}
        with self.assertRaises(LedgerError) as cm:
            results.collect_attempt(self.ledger, self.client, att["id"], config)
        self.assertIn(cm.exception.code, ("COLLECT_FAILED", "DELIVERABLE_CANDIDATE_INVALID"))
        with patch.object(results, "_hash_file_strict", return_value=(None, "read-failed")):
            with self.assertRaises(LedgerError) as cm:
                self.collect(att)
            self.assertIn(cm.exception.code, ("COLLECT_FAILED", "DELIVERABLE_CANDIDATE_INVALID"))
        self.assertEqual(self.ledger.get_attempt(att["id"]), before)
        self.assertEqual(self.ledger.deliverables(att["id"]), rows)
        self.assertEqual(self.rows(att), notices)

    def test_window_over_100_messages_excludes_later_round(self):
        _, att = self._attempt(deliverables=[])
        self._seal_baseline(att["id"])
        self._messages("ses_w", [f"回报 {i:03}" for i in range(150)], "msg_in", 1000)
        complete_execution(self.ledger, att["id"], input_at=1000, ended_at=2000)
        self.client.append_message("ses_w", {"id": "next-user", "type": "user",
                                               "time": {"created": 3000}})
        self.client.append_message("ses_w", _assistant("next", "后续回合", 3010))
        cfg = {**CFG, "result_max_messages": 17, "result_scan_max_pages": 20}
        out = results.collect_attempt(self.ledger, self.client, att["id"], cfg)
        self.assertEqual(out["assistant_message_count"], 150)
        snap = json.loads(Path(out["result_path"]).read_text())
        self.assertIn("回报 000", snap["text"])
        self.assertIn("回报 149", snap["text"])
        self.assertNotIn("后续回合", snap["text"])
        self.assertTrue(snap["message_window"]["complete"])

    def test_restart_preserves_retry_deadline_and_exhaustion_then_manual_recovers(self):
        cfg = {**CFG, "collect_max_attempts": 3, "auto_collect": True}
        task, att, first = self._collected(deliverables=(), texts=(), cfg=cfg)
        clock = FakeClock(wall_ms=first["next_collect_at"] - 1)
        sup = Supervisor(self.home, cfg=cfg, client=self.client, clock=clock)
        self.addCleanup(sup.conn.close)
        sup.finalize_terminals({})
        self.assertEqual(sup.ledger.get_attempt(att["id"])["metadata"]["collection"]["count"], 1)
        clock._wall = first["next_collect_at"]
        sup.finalize_terminals({})
        collection = sup.ledger.get_attempt(att["id"])["metadata"]["collection"]
        self.assertEqual(collection["count"], 2)
        restarted = Supervisor(self.home, cfg=cfg, client=self.client, clock=clock)
        self.addCleanup(restarted.conn.close)
        restarted.finalize_terminals({})
        self.assertEqual(restarted.ledger.get_attempt(att["id"])["metadata"]["collection"], collection)
        clock._wall = collection["next_collect_at"]
        restarted.finalize_terminals({})
        exhausted = self.ledger.get_attempt(att["id"])
        self.assertEqual(exhausted["status"], "result_missing")
        self.assertEqual(self.ledger.get_task(task["id"])["status"], "result_pending")
        clock.advance(seconds=3600)
        restarted.finalize_terminals({})
        self.assertEqual(self.ledger.get_attempt(att["id"])["metadata"]["collection"]["count"], 3)
        missing = [n for n in self.rows(att) if n["kind"] == "result_missing"]
        self.assertEqual(len(missing), 1)
        self.assertTrue(json.loads(missing[0]["payload"])["next_action"])
        self.client.append_message("ses_w", _assistant("late-visible", "真实完成", 1000040))
        out = self.collect(att)
        self.assertEqual(out["status"], "result_collected")
        self.assertEqual(self.ledger.get_task(task["id"])["status"], "result_received")

    def test_older_real_collection_cannot_overwrite_interleaved_newer_text(self):
        _, att, _ = self._collected(deliverables=(), texts=("第一版",))
        second = Ledger(init(self.home), self.home)
        self.addCleanup(second.conn.close)
        reader = results._CollectionIO.read_message_window
        fired = False
        newer = {}

        def interleave(*args, **kwargs):
            nonlocal fired
            window, coverage = copy.deepcopy(reader(*args, **kwargs))
            if not fired:
                fired = True
                self.client._replace_message("ses_w", _assistant("msg_in-a0", "第二版", 1000010))
                newer.update(results.collect_attempt(second, self.client, att["id"], CFG))
            return window, coverage

        with patch.object(results._CollectionIO, "read_message_window", side_effect=interleave):
            with self.assertRaises(LedgerError) as cm:
                self.collect(att)
        self.assertEqual(cm.exception.code, "COLLECTION_CONFLICT")
        current = self.ledger.get_attempt(att["id"])
        self.assertEqual(current["result_path"], newer["result_path"])
        self.assertEqual(json.loads(Path(current["result_path"]).read_text())["text"], "第二版")
        self.assertEqual(len([n for n in self.rows(att) if n["kind"] == "result_ready"]), 2)

    def test_same_version_interleaving_reuses_first_snapshot_time(self):
        _, att = self._attempt(deliverables=[])
        self._seal_baseline(att["id"])
        self._messages("ses_w", ["完成"], "msg_in", 1000)
        complete_execution(self.ledger, att["id"], input_at=1000, ended_at=2000)
        second = Ledger(init(self.home), self.home)
        self.addCleanup(second.conn.close)
        publisher = results._CollectionIO.publish_version_snapshot
        fired = False
        other = {}

        def interleave(*args, **kwargs):
            nonlocal fired
            published = publisher(*args, **kwargs)
            if not fired:
                fired = True
                other.update(results.collect_attempt(second, self.client, att["id"], CFG,
                                                     clock=lambda: 6000))
            return published

        with patch.object(results._CollectionIO, "publish_version_snapshot", side_effect=interleave):
            out = self.collect(att, clock=lambda: 5000)
        current = self.ledger.get_attempt(att["id"])
        self.assertEqual(out["result_version"], other["result_version"])
        self.assertEqual(out["notification_ids"], other["notification_ids"])
        self.assertEqual(current["result_collected_at"], 5000)
        self.assertEqual(json.loads(Path(current["result_path"]).read_text())["collected_at"], 5000)

    def test_controls_during_collection_io_reject_stale_commit(self):
        for action in ("paused", "cancelled", "new-current", "stop", "accepted"):
            with self.subTest(action=action):
                f = RemediationBase()
                f.setUp()
                try:
                    task, att, old = f._collected(final_contents={"out.txt": "v1"})
                    second = Ledger(init(f.home), f.home)
                    self.addCleanup(second.conn.close)
                    reader = results._CollectionIO.read_message_window

                    def interleave(*args, **kwargs):
                        data = reader(*args, **kwargs)
                        if action in ("paused", "cancelled"):
                            second.transition_task(task["id"], action, reason="test-control")
                        elif action == "new-current":
                            second.create_attempt(task["id"], directory=f.site + "-next",
                                                  session_id="ses_next", write_scope=[f.site + "-next"])
                        elif action == "stop":
                            second.stop_attempt(att["id"], actor="coordinator")
                        else:
                            results.verify_attempt(second, att["id"], result="pass")
                        return data

                    with patch.object(results._CollectionIO, "read_message_window", side_effect=interleave):
                        with self.assertRaises(LedgerError):
                            results.collect_attempt(f.ledger, f.client, att["id"], CFG)
                    self.assertEqual(f.ledger.get_attempt(att["id"])["result_path"], old["result_path"])
                    self.assertEqual(second.conn.execute(
                        "SELECT COUNT(*) FROM notifications WHERE kind='result_ready' AND attempt_id=?",
                        (att["id"],)).fetchone()[0], 1)
                finally:
                    f.doCleanups()

    def test_internal_scan_typeerror_is_not_retried_without_budget(self):
        _, att, _ = self._collected(final_contents={"out.txt": "v1"})
        with patch.object(results._CollectionIO, "compare_provenance",
                          side_effect=TypeError("scan internal bug")) as scan:
            with self.assertRaises(TypeError):
                self.collect(att)
        self.assertEqual(scan.call_count, 1)

    def test_failed_checks_and_changed_inputs_are_not_reported_ready(self):
        _, att, first = self._collected(
            deliverables=["out.txt", {"kind": "command", "target": "false"}],
            final_contents={"out.txt": "v1"})
        self.assertEqual(first["classification"], "CHECKS_PENDING")
        results.run_checks(self.ledger, att["id"])
        failed = self.collect(att)
        self.assertEqual(failed["classification"], "CHECKS_FAILED")
        self.assertFalse(failed["ready_for_verification"])
        self.assertNotEqual(first["result_version"], failed["result_version"])
        self._write("out.txt", "v2")
        changed = self.collect(att)
        self.assertEqual(changed["classification"], "CHECKS_PENDING")
        self.assertFalse(changed["ready_for_verification"])


class LegacyBridgeAcceptanceTests(LegacyBase):
    def fixture(self, state="acked"):
        task, att = self.make_legacy_damage(deliverables=())
        text = "同一次有效完成"
        path = Path(self.home) / f"{att['id']}-legacy.json"
        doc = {"schema": "oc-orch.result.v1", "attempt_id": att["id"],
               "task_id": task["id"], "session_id": "ses_w",
               "generation": att["generation"], "collected_at": 2500,
               "since": 1000, "auto": True, "assistant_message_count": 1,
               "text_chars": len(text), "text_truncated": False,
               "text": text, "tools": [], "terminal_kind": "succeeded"}
        path.write_text(json.dumps(doc))
        # 明确构造旧版有效历史；与早产/逆序事故夹具区别开。
        self.ledger.conn.execute(
            "UPDATE attempts SET status='result_collected', terminal_at=2000, "
            "result_collected_at=2500, result_path=? WHERE id=?", (str(path), att["id"]))
        self.ledger.conn.execute("UPDATE turns SET ended_at=2000 WHERE attempt_id=?", (att["id"],))
        self.client._messages_store["ses_w"] = []
        self.client.append_message("ses_w", {"id": "msg_in", "type": "user", "time": {"created": 1000}})
        self.client.append_message("ses_w", {"id": "final", "type": "assistant",
            "time": {"created": 1500, "completed": 1900},
            "content": [{"type": "text", "text": text}]})
        self.client.add_idle("ses_w", msg_id="idle", at=2000, outcome="succeeded")
        self.rest_success(idle=2000)
        notif = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c", task_id=task["id"],
            attempt_id=att["id"], project="p", dedupe_key=f"result-ready:{att['id']}",
            next_attempt_at=self.clock.now_ms() + 10000,
            payload={"task_id": task["id"], "attempt_id": att["id"], "session_id": "ses_w",
                     "classification": "RESULT_READY", "result_path": str(path),
                     "missing_deliverables": []})
        if state != "pending":
            self.ledger.claim_notifications([notif["id"]], "test")
            self.ledger.mark_notification_sent(notif["id"], message_id="old-wake",
                next_attempt_at=self.clock.now_ms() + 30000, owner="test")
        if state == "acked":
            self.ledger.ack_notifications([notif["id"]], actor="read-old")
        elif state == "dead":
            self.ledger.mark_notification_dead(notif["id"], error="exhausted", owner="test")
        return att, path, doc, self.ledger.get_notification(notif["id"])

    def test_real_v1_facade_bridge_preserves_identity_budget_and_ack(self):
        for state in ("pending", "sent", "acked", "dead"):
            with self.subTest(state=state):
                att, path, doc, old = self.fixture(state)
                turns = self.ledger.turns(att["id"])
                out = self.app.collect(att["id"])
                again = self.app.collect(att["id"])
                self.assertEqual(out["notification_ids"], [old["id"]])
                self.assertEqual(again["notification_ids"], [old["id"]])
                self.assertEqual(out["result_version"], again["result_version"])
                after = self.ledger.get_notification(old["id"])
                for field in ("state", "attempts", "next_attempt_at", "acked_at", "sent_at"):
                    self.assertEqual(after[field], old[field], field)
                self.assertEqual(len(self.notifications(att["id"])), 1)
                self.assertEqual(self.ledger.turns(att["id"]), turns)
                self.assertEqual(json.loads(path.read_text()), doc)

    def test_unproven_legacy_binding_cannot_suppress_new_version(self):
        for case in ("early-terminal", "early-collection", "wrong-since", "bad-body",
                     "missing-path", "wrong-session", "early-notification", "unknown-files"):
            with self.subTest(case=case):
                att, path, doc, old = self.fixture()
                if case == "early-terminal":
                    self.ledger.conn.execute("UPDATE attempts SET terminal_at=999 WHERE id=?", (att["id"],))
                elif case == "early-collection":
                    doc["collected_at"] = 1500
                elif case == "wrong-since":
                    doc["since"] = 900
                elif case == "bad-body":
                    doc["text_full_sha256"] = self.sha(doc["text"])
                    doc["text"] = "伪造正文"
                elif case == "unknown-files":
                    doc["deliverables"] = [{"target": "report.md", "provenance": "unknown"}]
                elif case == "early-notification":
                    self.ledger.conn.execute("UPDATE notifications SET created_at=999 WHERE id=?", (old["id"],))
                else:
                    payload = dict(old["payload"])
                    payload["result_path" if case == "missing-path" else "session_id"] = None
                    self.ledger.conn.execute("UPDATE notifications SET payload=? WHERE id=?",
                                             (json.dumps(payload), old["id"]))
                path.write_text(json.dumps(doc))
                before = self.ledger.get_notification(old["id"])
                out = self.app.collect(att["id"])
                self.assertNotEqual(out["notification_ids"], [old["id"]])
                self.assertEqual(self.ledger.get_notification(old["id"]), before)
                self.assertEqual(len(self.notifications(att["id"])), 2)

    def test_legacy_bridge_is_not_hidden_by_unrelated_notification_limit(self):
        att, _, _, old = self.fixture()
        self.clock.advance(seconds=1)
        for i in range(205):
            self.ledger.enqueue_notification(kind="other", target_session="ses_c",
                                            dedupe_key=f"noise-{i}", payload={})
        out = self.app.collect(att["id"])
        self.assertEqual(out["notification_ids"], [old["id"]])

    def test_legacy_recovery_rejects_ambiguous_or_unknown_execution(self):
        for case in ("later-input", "second-idle", "no-idle", "wrong-session",
                     "malformed-busy", "unknown-inbox"):
            with self.subTest(case=case):
                att, _, _, old = self.fixture()
                if case == "later-input":
                    self.client.append_message("ses_w", {"id": "other-input", "type": "user",
                                                          "time": {"created": 1400}})
                elif case == "second-idle":
                    self.client.add_idle("ses_w", outcome="succeeded", at=1600)
                elif case == "no-idle":
                    self.client._messages_store["ses_w"] = [m for m in
                        self.client._messages_store["ses_w"] if m["type"] != "idle"]
                elif case == "wrong-session":
                    self.client._messages_store["ses_w"][0]["sessionID"] = "ses_other"
                elif case == "malformed-busy":
                    self.rest_success(time_override={"busy": "bad", "idle": 2000})
                else:
                    self.client.inbox = None
                before = self.ledger.get_attempt(att["id"])
                with self.assertRaises(LedgerError) as cm:
                    results.collect_attempt(self.ledger, self.client, att["id"], CFG)
                self.assertEqual(cm.exception.code, "NOT_READY")
                self.assertEqual(self.ledger.get_attempt(att["id"]), before)
                self.assertEqual(self.ledger.get_notification(old["id"]), old)

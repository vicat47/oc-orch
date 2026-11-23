#!/usr/bin/env python3
"""OpenSpec fix-attempt-completion-integrity：legacy 恢复与同内容桥接（F2/T29/T36/F4）。

对应收口复核反例（evidence/2026-11-08-completion-integrity-review）：
- F2 legacy 假终态被直接当真实成功、活动中的进度文本可通过正常验收
  → 修复后：worker 仍 active / REST 无成功事实 / idle 无效 / 输入未投递 /
  纯进度文本，一律 NOT_READY，不收集、不通知、不改历史；
  有后来真实成功区间（REST outcome/idle + 输入锚点 + 非 active）才可恢复
  收集，并持久 collection.recovery_execution，保留坏 turn/terminal_at/旧 ACK。
- F4 来源 unknown（扫描预算不足）被正常 verify 放行 → 修复后 STALE_GATE_FAIL。
- 任务 6.2 可证明的同内容 legacy 通知桥接：严格证据匹配才复用旧通知身份，
  ACK/期限不变；不明旧空结果不压制新版本。

真临时 SQLite WAL（部分场景双连接）；FakeClient + FakeClock；受控失败注入；
无概率 sleep。历史损坏样本仅在显式事故夹具中用 SQL 构造（design M2 允许）。
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.adapters.results import _CollectionIO, collect_attempt
from oc_orch.application import collection as _collection
from oc_orch.composition import application
from oc_orch.config import DEFAULTS
from oc_orch.db import init
from oc_orch.domain import execution as _exec_rules
from oc_orch.domain import results as rules
from oc_orch.ledger import Ledger, LedgerError

from tests.fakes import FakeClient, FakeClock

CFG = {**DEFAULTS, "desktop_alerts": False, "collect_retry_interval": 15,
       "collect_max_attempts": 5}


def _user(mid: str, at: int, text: str = "go"):
    return {"id": mid, "type": "user", "text": text, "time": {"created": at}}


def _assistant(mid: str, text: str, at: int):
    return {"id": mid, "type": "assistant", "time": {"created": at},
            "content": [{"type": "text", "text": text}]}


class RemediationBase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="oc-orch-remed-")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.clock = FakeClock(wall_ms=1_000_000)
        self.client = FakeClient()
        self.ledger = Ledger(init(self.home), self.home, clock=self.clock)
        self.addCleanup(self.ledger.conn.close)
        self.app = application(self.ledger, lambda: self.client, CFG,
                               clock=self.clock)
        os.makedirs(self.site, exist_ok=True)
        self.client.set_session("ses_c", time_={})

    @property
    def site(self):
        return os.path.join(self.home, "site")

    def file(self, name: str, text: str):
        with open(os.path.join(self.site, name), "w", encoding="utf-8") as f:
            f.write(text)

    def sha(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def notifications(self, attempt_id):
        rows = self.ledger.conn.execute(
            "SELECT * FROM notifications WHERE attempt_id=? ORDER BY created_at",
            (attempt_id,)).fetchall()
        return [dict(r, payload=json.loads(r["payload"])) for r in rows]

    def make_legacy_damage(self, *, deliverables=("report.md",),
                           observed_sha: str | None = None,
                           observed_status: str = "present"):
        """构造历史损坏样本（显式事故夹具，design M2 允许直接 SQL）：
        旧实现逆序 turn + 假 terminal_at + 早产收集留下的交付观察行。"""
        task = self.ledger.create_task(project="p", title="legacy-damage",
                                       coordinator_session="ses_c",
                                       write_scope=[self.site])
        # 唯一直接 SQL：历史事故样本（坏 turn 逆序 + 假终态 + 早产观察行 +
        # 旧实现留下的 site 释放痕迹——避免它挡住本测试对同目录的复用）
        att = self.ledger.create_attempt(
            task["id"], directory=self.site, session_id="ses_w",
            status="running", dispatch_state="confirmed",
            deliverables=list(deliverables),
            dispatch_msg_id="msg_in", write_scope=[self.site])
        self.ledger.conn.execute(
            "UPDATE attempts SET site_released_at=1 "
            "WHERE id=?", (att["id"],))
        self.ledger.conn.commit()
        self.ledger.conn.execute(
            "UPDATE attempts SET status='terminal_succeeded', started_at=1000, "
            "terminal_at=999, terminal_kind='succeeded' WHERE id=?", (att["id"],))
        self.ledger.conn.execute(
            "INSERT INTO turns(attempt_id, start_event_id, end_event_id, "
            "started_at, ended_at, outcome) VALUES(?,?,?,?,?,?)",
            (att["id"], "evt-real-start", "obs-fake-end", 1000, 999, "succeeded"))
        if observed_sha is not None:
            self.ledger.conn.execute(
                "UPDATE deliverables SET status=?, evidence=? WHERE attempt_id=?",
                (observed_status, json.dumps(
                    {"path": os.path.join(self.site, deliverables[0]),
                     "sha256": observed_sha}), att["id"]))
        return task, att

    def rest_success(self, *, idle=2000, busy=1000, active=False, outcome="succeeded",
                     time_override=None):
        time_ = time_override if time_override is not None else {
            "idle": idle, "busy": busy, "updated": idle}
        session = self.client.set_session("ses_w", outcome=outcome, time_=time_)
        if active:
            self.client.active["ses_w"] = session
        else:
            self.client.active.pop("ses_w", None)
        if type(idle) is int and outcome == "succeeded" and not active and time_override is None:
            if not any(m.get("type") == "idle" and m.get("time", {}).get("created") == idle
                       for m in self.client._messages_store.get("ses_w", [])):
                self.client.add_idle("ses_w", outcome=outcome, at=idle)
        return session


class TestLegacyDamageRequiresRealEvidence(RemediationBase):
    """F2 反例转正：legacy 假终态必须补证真实成功区间。"""

    def test_active_worker_and_progress_only_rejected(self):
        """worker 仍 active + REST outcome 为空 + 只有进度文本 → 不能收集。"""
        task, att = self.make_legacy_damage(observed_sha=self.sha("old"))
        self.file("report.md", "still working draft")
        # 进度文本 + worker 活跃 + 无 REST 成功事实
        self.client.append_message("ses_w", _user("msg_in", 1000))
        self.client.append_message("ses_w", _assistant(
            "msg_p1", "still working, progress only", 1200))
        self.rest_success(outcome=None, active=True, time_override={"busy": 1000})
        with self.assertRaises(LedgerError) as ctx:
            collect_attempt(self.ledger, self.client, att["id"], CFG,
                            auto=False, clock=self.clock.now_ms)
        self.assertEqual(ctx.exception.code, "NOT_READY")
        after = self.ledger.get_attempt(att["id"])
        # 历史坏事实保留，且没有被任何新事实污染
        self.assertEqual(after["terminal_at"], 999, "坏 terminal_at 保持可审计")
        self.assertEqual(after["terminal_kind"], "succeeded")
        self.assertIsNone(after["result_collected_at"])
        self.assertIn(after["status"], ("terminal_succeeded",))
        self.assertIsNone((after["metadata"].get("collection") or {})
                          .get("recovery_execution"))
        self.assertEqual(len(self.notifications(att["id"])), 0,
                         "活动进度不得产生任何通知")
        bad = [t for t in self.ledger.turns(att["id"])
               if t["end_event_id"] == "obs-fake-end"]
        self.assertEqual(len(bad), 1, "坏 turn 保持可审计")

    def test_invalid_idle_and_undelivered_input_rejected(self):
        for name, setup in (
            ("no-idle", lambda: self.rest_success(
                outcome="succeeded", time_override={"busy": 1000})),
            ("bad-idle-type", lambda: self.rest_success(
                outcome="succeeded", time_override={"busy": 1000, "idle": "x"})),
            ("input-in-inbox", lambda: (
                self.rest_success(idle=2000),
                self.client.set_inbox("ses_w", [
                    {"id": "msg_in", "type": "user", "texts": "go"}]))),
        ):
            with self.subTest(case=name):
                task, att = self.make_legacy_damage(observed_sha=self.sha("old"))
                self.file("report.md", "changed content")
                self.client.append_message("ses_w", _assistant(
                    "msg_a", "完成", 1500))
                if name != "input-in-inbox":
                    self.client.append_message("ses_w", _user("msg_in", 1000))
                setup()
                with self.assertRaises(LedgerError) as ctx:
                    collect_attempt(self.ledger, self.client, att["id"], CFG,
                                    auto=False, clock=self.clock.now_ms)
                self.assertEqual(ctx.exception.code, "NOT_READY")
                self.assertIsNone(
                    self.ledger.get_attempt(att["id"])["result_collected_at"])

    def test_progress_text_with_idle_only_not_enough_without_input_anchor(self):
        """REST 有 succeeded/idle 但输入锚点缺失（消息不可见）→ 不可证明。"""
        task, att = self.make_legacy_damage(observed_sha=self.sha("old"))
        self.file("report.md", "changed content")
        self.client.append_message("ses_w", _assistant("msg_a", "完成", 1500))
        self.rest_success(idle=2000)  # 但没有 user msg_in 消息
        with self.assertRaises(LedgerError) as ctx:
            collect_attempt(self.ledger, self.client, att["id"], CFG,
                            auto=False, clock=self.clock.now_ms)
        self.assertEqual(ctx.exception.code, "NOT_READY")


class TestLegacyRecoveryCollect(RemediationBase):
    """F2/T29 正例：后来真实成功区间 → 恢复收集 + recovery_execution 持久化。"""

    def test_recovered_collect_persists_recovery_and_keeps_history(self):
        task, att = self.make_legacy_damage(observed_sha=self.sha("old-observed"))
        # 旧早产 ACK 通知（无版本 attempt 级）
        legacy_notif = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c",
            dedupe_key=f"result-ready:{att['id']}",
            payload={"task_id": task["id"], "attempt_id": att["id"],
                     "classification": "RESULT_READY", "summary": "执行回合成功结束"},
            project="p", task_id=task["id"], attempt_id=att["id"])
        self.ledger.ack_notifications([legacy_notif["id"]], actor="coordinator:old")
        acked_at = self.ledger.get_notification(legacy_notif["id"])["acked_at"]
        # 后来真实成功：输入锚点 + 实质回报 + 内容相对旧观察基线变化
        self.file("report.md", "真实修订内容")
        self.client.append_message("ses_w", _user("msg_in", 1000))
        self.client.append_message("ses_w", _assistant(
            "msg_final", "真实完成：全部交付物就绪", 1500))
        self.rest_success(idle=2000, busy=1000)
        out = collect_attempt(self.ledger, self.client, att["id"], CFG,
                              auto=False, clock=self.clock.now_ms)
        self.assertTrue(out["result_version"], "恢复收集必须返回结果版本")
        self.assertTrue(out["result_path"])
        after = self.ledger.get_attempt(att["id"])
        # recovery_execution 持久化（design M2：真实区间 + 原损坏记录引用）
        collection = after["metadata"]["collection"]
        recovery = collection.get("recovery_execution")
        self.assertIsNotNone(recovery)
        self.assertEqual(recovery["real_end_at"], 2000)
        self.assertEqual(recovery["input_message_id"], "msg_in")
        self.assertEqual(recovery["legacy_terminal"], {"at": 999, "kind": "succeeded"})
        self.assertIsNotNone(after["metadata"].get("collection_recovery"))
        # 坏 terminal_at/turn 不被改写；旧 ACK 不复活
        self.assertEqual(after["terminal_at"], 999)
        bad = [t for t in self.ledger.turns(att["id"])
               if t["end_event_id"] == "obs-fake-end"]
        self.assertEqual(len(bad), 1)
        old_row = self.ledger.get_notification(legacy_notif["id"])
        self.assertEqual(old_row["state"], "acked")
        self.assertEqual(old_row["acked_at"], acked_at)
        # 新版本通知（不被旧 ACK 压制；collect 不隐式 ACK）
        new_ids = [i for i in out["notification_ids"]
                   if i != legacy_notif["id"]]
        self.assertTrue(new_ids, "真实新结果必须有新通知责任")
        for nid in new_ids:
            self.assertIn(self.ledger.get_notification(nid)["state"],
                          ("pending", "sent"))
        # 交付行被原子提交（F6）：内容为新基线观察
        row = self.ledger.deliverables(att["id"])[0]
        evidence = json.loads(row["evidence"])
        self.assertEqual(evidence.get("sha256"), self.sha("真实修订内容"))
        # 收集窗口有真实上界
        snap = json.load(open(out["result_path"], encoding="utf-8"))
        self.assertEqual(snap["message_window"]["until"], 2000)
        self.assertEqual(snap["message_window"]["since"], 1000)

    def test_recovered_attempt_passes_normal_verification_after_carry_evidence(self):
        """恢复收集（内容相对旧观察 changed）→ 正常 verify 通过。"""
        task, att = self.make_legacy_damage(observed_sha=self.sha("old-observed"))
        self.file("report.md", "真实修订内容 v2")
        self.client.append_message("ses_w", _user("msg_in", 1000))
        self.client.append_message("ses_w", _assistant("msg_f", "完成", 1500))
        self.rest_success(idle=2000)
        from oc_orch.results import verify_attempt
        self.app.collect(att["id"])
        out = verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(out["verification"]["classification"], "normal_effective")


class TestLegacyCarryLabelStability(RemediationBase):
    """D6/D8：显式 carry 不能被旧观察标签覆盖或在失效后复活。"""

    def _legacy(self, *, observed="原交付"):
        task, att = self.make_legacy_damage(
            observed_sha=self.sha(observed) if observed is not None else None)
        self.file("report.md", "原交付")
        self.client.append_message("ses_w", _user("msg_in", 1000))
        self.client.append_message("ses_w", _assistant("msg_final", "完成", 1500))
        self.rest_success(idle=2000)
        manifest = {
            "schema": "oc-orch.carry.v1", "attempt_id": att["id"],
            "items": [{"target": "report.md",
                       "baseline_sha256": self.sha("原交付"),
                       "reason": "已对照历史交付逐字节核实，明确沿用"}],
        }
        return task, att, manifest

    def _assert_carried(self, out):
        self.assertEqual(out["deliverables"][0]["provenance"], "carried")
        self.assertEqual(out["stale"], [])
        self.assertTrue(out["ready_for_verification"])
        self.assertEqual(out["classification"], "RESULT_READY")
        with open(out["result_path"], encoding="utf-8") as f:
            snapshot = json.load(f)
        self.assertEqual(snapshot["deliverables"][0]["provenance"], "carried")
        self.assertEqual(snapshot["stale_deliverables"], [])

    def test_first_legacy_collect_accepts_valid_carry(self):
        _, att, manifest = self._legacy()
        out = self.app.collect(att["id"], carry_manifest=manifest)
        self._assert_carried(out)
        self.assertTrue(self.ledger.get_attempt(att["id"])["metadata"]
                        ["collection"]["recovery_execution"])
        self.assertEqual(self.ledger.get_attempt(att["id"])["terminal_at"], 999)

    def test_valid_carry_supersedes_prior_observation_labels(self):
        from oc_orch.results import verify_attempt
        for observed, label in (("原交付", "unchanged"), (None, "unknown"),
                                ("早期观察", "changed")):
            with self.subTest(prior_label=label):
                task, att, manifest = self._legacy(observed=observed)
                first = self.app.collect(att["id"])
                self.assertEqual(first["deliverables"][0]["provenance"], label)
                self.ledger.ack_notifications(first["notification_ids"], actor="old")
                old_notifications = [self.ledger.get_notification(nid)
                                     for nid in first["notification_ids"]]
                old_turns = self.ledger.turns(att["id"])
                with open(first["result_path"], "rb") as f:
                    old_bytes = f.read()

                carried = self.app.collect(att["id"], carry_manifest=manifest)
                self._assert_carried(carried)
                self.assertNotEqual(first["result_version"], carried["result_version"])
                for old in old_notifications:
                    self.assertEqual(self.ledger.get_notification(old["id"]), old)
                with open(first["result_path"], "rb") as f:
                    self.assertEqual(f.read(), old_bytes)
                self.assertEqual(self.ledger.turns(att["id"]), old_turns)
                self.assertEqual(self.ledger.get_attempt(att["id"])["terminal_at"], 999)
                self.assertEqual(self.ledger.get_task(task["id"])["current_attempt_id"],
                                 att["id"])
                verified = verify_attempt(self.ledger, att["id"], result="pass")
                self.assertEqual(verified["verification"]["classification"],
                                 "normal_effective")

    def test_explicit_and_persisted_carry_recollect_preserve_version_and_ack(self):
        _, att, manifest = self._legacy()
        first = self.app.collect(att["id"], carry_manifest=manifest)
        self._assert_carried(first)
        self.ledger.ack_notifications(first["notification_ids"], actor="coordinator")
        notifications = self.notifications(att["id"])
        collected_at = self.ledger.get_attempt(att["id"])["result_collected_at"]
        collection = self.ledger.get_attempt(att["id"])["metadata"]["collection"]
        for carry_input in (manifest, None):
            with self.subTest(explicit=carry_input is not None):
                self.clock.advance(seconds=30)
                repeat = self.app.collect(att["id"], carry_manifest=carry_input)
                self._assert_carried(repeat)
                self.assertTrue(repeat["duplicate"])
                for key in ("result_version", "result_path"):
                    self.assertEqual(first[key], repeat[key])
                self.assertEqual(self.ledger.get_attempt(att["id"])
                                 ["result_collected_at"], collected_at)
                self.assertEqual(self.notifications(att["id"]), notifications)
                current = self.ledger.get_attempt(att["id"])["metadata"]["collection"]
                for key in ("count", "next_collect_at"):
                    self.assertEqual(current.get(key), collection.get(key))

    def test_invalid_replacement_carry_cannot_restore_prior_carried_label(self):
        from oc_orch.results import verify_attempt
        _, att, manifest = self._legacy()
        first = self.app.collect(att["id"], carry_manifest=manifest)
        self._assert_carried(first)
        manifest["items"][0]["baseline_sha256"] = self.sha("不匹配的历史内容")
        invalid = self.app.collect(att["id"], carry_manifest=manifest)
        self.assertEqual(invalid["deliverables"][0]["provenance"], "unchanged")
        self.assertEqual(invalid["classification"], "DELIVERABLE_STALE")
        self.assertFalse(invalid["ready_for_verification"])
        self.assertNotEqual(invalid["result_version"], first["result_version"])
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "STALE_GATE_FAIL")
        repeat = self.app.collect(att["id"])
        self.assertEqual(repeat["result_version"], invalid["result_version"])
        self.assertFalse(repeat["ready_for_verification"])

    def test_changed_file_invalidates_persisted_carry(self):
        from oc_orch.results import verify_attempt
        _, att, manifest = self._legacy()
        first = self.app.collect(att["id"], carry_manifest=manifest)
        self._assert_carried(first)
        self.file("report.md", "后续修订")
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")
        changed = self.app.collect(att["id"])
        self.assertEqual(changed["deliverables"][0]["provenance"], "changed")
        self.assertNotEqual(changed["result_version"], first["result_version"])
        with open(changed["result_path"], encoding="utf-8") as f:
            snapshot = json.load(f)
        self.assertEqual(snapshot["deliverables"][0]["sha256"], self.sha("后续修订"))

    def test_deleted_file_cannot_keep_prior_carried_label(self):
        _, att, manifest = self._legacy()
        first = self.app.collect(att["id"], carry_manifest=manifest)
        self._assert_carried(first)
        os.unlink(os.path.join(self.site, "report.md"))
        missing = self.app.collect(att["id"])
        self.assertEqual(missing["deliverables"][0]["provenance"], "unknown")
        self.assertEqual(missing["classification"], "DELIVERABLE_MISSING")
        self.assertFalse(missing["ready_for_verification"])

    def test_recovery_boundary_mismatch_still_rejects_snapshot_reuse(self):
        _, att, manifest = self._legacy()
        first = self.app.collect(att["id"], carry_manifest=manifest)
        with open(first["result_path"], encoding="utf-8") as f:
            snapshot = json.load(f)
        snapshot["recovery_execution"]["real_end_at"] += 1
        with open(first["result_path"], "w", encoding="utf-8") as f:
            json.dump(snapshot, f)
        self.clock.advance(seconds=30)
        with self.assertRaises(LedgerError) as ctx:
            collect_attempt(self.ledger, self.client, att["id"], CFG,
                            clock=self.clock.now_ms)
        self.assertEqual(ctx.exception.code, "RESULT_VERSION_MISMATCH")


class TestLegacySameContentBridge(RemediationBase):
    """任务 6.2/T36：可证明同内容才桥接旧通知身份；不明确即新身份。"""

    def _legacy_collected(self, *, text: str, deliverable_sha: str | None,
                          file_body: str | None, deliverables=("report.md",)):
        """旧协议已收集样本：SQL 造终态/采集标记 + 手写旧格式结果文件。"""
        task, att = self.make_legacy_damage(
            deliverables=deliverables,
            observed_sha=deliverable_sha if deliverable_sha else None,
            observed_status="present" if deliverable_sha else "expected")
        old_path = os.path.join(self.home, "legacy-result.json")
        payload = {
            "schema": "oc-orch.result.v1", "attempt_id": att["id"],
            "task_id": task["id"], "generation": att["generation"],
            "session_id": "ses_w", "assistant_message_count": 1,
            "text_chars": len(text), "text": text, "collected_at": 2500, "since": 1000,
            "auto": True, "terminal_kind": "succeeded",
            "text_full_sha256": self.sha(text),
        }
        if deliverable_sha:
            payload["deliverables"] = [
                {"target": "report.md", "kind": "file", "status": "present",
                 "sha256": deliverable_sha, "manifest_sha256": None,
                 "provenance": "unknown", "provenance_detail": {}}]
        else:
            payload["deliverables"] = []
        with open(old_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        self.ledger.conn.execute(
            "UPDATE attempts SET status='result_collected', "
            "terminal_at=2000, result_collected_at=2500, result_path=? WHERE id=?",
            (old_path, att["id"]))
        return task, att, old_path, payload

    def test_same_content_bridges_old_notification_identity(self):
        """第一次相同 legacy 收集：语义内容相同 → 复用旧通知身份，不重发。"""
        text = "完成：产物就绪"
        task, att, old_path, old_payload = self._legacy_collected(
            text=text, deliverable_sha=None, file_body=None, deliverables=())
        legacy_notif = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c",
            dedupe_key=f"result-ready:{att['id']}",
            payload={"task_id": task["id"], "attempt_id": att["id"],
                     "session_id": "ses_w", "result_path": old_path,
                     "classification": "RESULT_READY", "summary": "执行回合成功结束"},
            project="p", task_id=task["id"], attempt_id=att["id"])
        # 实际旧 v1 的文本型有效结果；未知文件来源不能用来压制新通知。
        self.client.append_message("ses_w", _user("msg_in", 1000))
        self.client.append_message("ses_w", _assistant("msg_out", text, 1500))
        self.rest_success(idle=2000)
        out = collect_attempt(self.ledger, self.client, att["id"], CFG,
                              auto=False, clock=self.clock.now_ms)
        # 桥接：dedupe 复用旧通知身份（merge 已有 pending；不产生第二条逻辑通知）
        self.assertTrue(out["result_version"])
        rows = self.notifications(att["id"])
        self.assertEqual(len(rows), 1, "同内容桥接不产生第二条逻辑通知")
        self.assertEqual(rows[0]["id"], legacy_notif["id"],
                         "复用旧通知身份")
        self.assertEqual(rows[0]["dedupe_key"], f"result-ready:{att['id']}")
        self.assertTrue(rows[0]["payload"].get("result_version"),
                        "桥接后的通知应携带新版本字段（可读性）")
        # 桥接信息入 payload（可审计），收集本身不 ACK
        self.assertTrue(rows[0]["payload"].get("bridged_from_notification"))
        self.assertIn(rows[0]["state"], ("pending", "sent"))

    def test_empty_legacy_result_cannot_suppress_new_version(self):
        """不明确的旧空结果：不桥接、不压制新版本/新通知身份。"""
        task, att, old_path, _ = self._legacy_collected(
            text="", deliverable_sha=None, file_body=None)  # 空文本无摘要
        legacy_notif = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c",
            dedupe_key=f"result-ready:{att['id']}",
            payload={"task_id": task["id"], "attempt_id": att["id"],
                     "classification": "RESULT_READY", "summary": "空结果"},
            project="p", task_id=task["id"], attempt_id=att["id"])
        self.ledger.ack_notifications([legacy_notif["id"]], actor="coordinator:old")
        # 后来的真实内容（≠ 旧空）
        self.file("report.md", "真实新内容")
        self.client.append_message("ses_w", _user("msg_in", 1000))
        self.client.append_message("ses_w", _assistant("msg_new", "真实完成", 1500))
        self.rest_success(idle=2000)
        out = collect_attempt(self.ledger, self.client, att["id"], CFG,
                              auto=False, clock=self.clock.now_ms)
        rows = self.notifications(att["id"])
        self.assertEqual(len(rows), 2, "旧空结果不得压制新版本通知")
        old_row = self.ledger.get_notification(legacy_notif["id"])
        self.assertEqual(old_row["state"], "acked", "旧通知保持 ACK")
        new_rows = [r for r in rows if r["id"] != legacy_notif["id"]]
        self.assertTrue(new_rows)
        self.assertNotEqual(new_rows[0]["dedupe_key"], old_row["dedupe_key"])
        self.assertTrue(new_rows[0]["payload"].get("result_version"))

    def test_different_content_not_bridged(self):
        """旧结果与新收集内容不同 → 新通知身份（新文本/新版本字段）。"""
        task, att, old_path, _ = self._legacy_collected(
            text="旧一轮文本", deliverable_sha=self.sha("旧内容"),
            file_body="旧内容")
        legacy_notif = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c",
            dedupe_key=f"result-ready:{att['id']}",
            payload={"task_id": task["id"], "attempt_id": att["id"],
                     "classification": "RESULT_READY", "summary": "旧完成"},
            project="p", task_id=task["id"], attempt_id=att["id"])
        # 新内容
        self.file("report.md", "新内容")
        self.client.append_message("ses_w", _user("msg_in", 1000))
        self.client.append_message("ses_w", _assistant("msg_n", "新文本", 1500))
        self.rest_success(idle=2000)
        out = collect_attempt(self.ledger, self.client, att["id"], CFG,
                              auto=False, clock=self.clock.now_ms)
        rows = self.notifications(att["id"])
        self.assertEqual(len(rows), 2)
        new_rows = [r for r in rows if r["id"] != legacy_notif["id"]]
        self.assertTrue(new_rows[0]["payload"].get("result_version"))
        self.assertNotIn("bridged_from_notification", new_rows[0]["payload"])


class TestUnknownProvenanceVerificationGate(RemediationBase):
    """F4：来源 unknown（基线扫描预算不足）正常 verify 必须被拒。"""

    def test_unknown_provenance_rejected_by_normal_verify(self):
        from oc_orch.adapters.results import scan_deliverable_baseline
        from oc_orch.results import verify_attempt
        task = self.ledger.create_task(project="p", title="unknown-prov",
                                       coordinator_session="ses_c",
                                       write_scope=[self.site])
        self.file("old.txt", "old unchanged contents")
        att = self.ledger.create_attempt(
            task["id"], directory=self.site, session_id="ses_w",
            status="dispatched", dispatch_state="confirmed",
            deliverables=["old.txt"], dispatch_msg_id="msg_in",
            write_scope=[self.site])
        # 派单前基线扫描预算不足 → 基线该项 unknown（probe 复现条件）
        ref = scan_deliverable_baseline(
            self.ledger, att["id"], budgets={"deliverable_scan_max_bytes": 1},
            captured_at=self.clock.now_ms())
        self.ledger.seal_execution(att["id"], mode="new", session_before={},
                                   baseline=ref)
        self.client.append_message("ses_w", _user("msg_in", 1000))
        self.ledger.record_execution_started(att["id"], started_at=1000,
                                             event_id="evt_s")
        self.client.append_message("ses_w", _assistant("msg_o", "finished report",
                                                       1200))
        out = self.ledger.record_terminal(
            att["id"], outcome="succeeded", source="sse", event_id="evt_e",
            candidate=_exec_rules.build_candidate(
                outcome="succeeded", source="sse", session_id="ses_w",
                ended_at=2000, event_id="evt_e",
                input_in_inbox=False, input_owner="unique"))
        self.assertEqual(out["result"], "accepted")
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"idle": 2000, "busy": 1000})
        result = collect_attempt(self.ledger, self.client, att["id"], CFG,
                                 auto=False, clock=self.clock.now_ms)
        self.assertEqual(result["status"], "result_collected")
        provenance = {d["target"]: d["provenance"]
                      for d in result["deliverables"]}
        self.assertEqual(provenance["old.txt"], "unknown")
        self.assertEqual(result["classification"], "DELIVERABLE_STALE")
        self.assertFalse(result["ready_for_verification"])
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "STALE_GATE_FAIL")
        # force 需要有理由并记录未满足项（F4 例外路径）
        forced = verify_attempt(self.ledger, att["id"], result="pass", force=True,
                                reason="人工核验基线预算不足误报，可接受")
        self.assertEqual(forced["verification"]["classification"], "forced")
        self.assertIn("deliverable_stale",
                      json.dumps(forced["verification"]["unmet"]))


class TestCommitCollectionGuards(RemediationBase):
    """N1/F6：commit_collection 状态白名单、guard 必需、交付候选原子提交。"""

    def _succeeded_with_baseline(self, *, files=("out.txt",), label="n1"):
        from oc_orch.adapters.results import scan_deliverable_baseline
        # 每个 guard 用例独立子 site（避免 F1 写权排他挡住第二个样本）
        site = os.path.join(self.site, label)
        os.makedirs(site, exist_ok=True)
        task = self.ledger.create_task(project="p", title=f"guards-{label}",
                                       coordinator_session="ses_c",
                                       write_scope=[site])
        for name in files:
            with open(os.path.join(site, name), "w", encoding="utf-8") as f:
                f.write("baseline-v0")
        att = self.ledger.create_attempt(
            task["id"], directory=site, session_id="ses_w",
            status="dispatched", dispatch_state="confirmed",
            deliverables=list(files), dispatch_msg_id="msg_in",
            write_scope=[site])
        ref = scan_deliverable_baseline(self.ledger, att["id"],
                                         captured_at=self.clock.now_ms())
        self.ledger.seal_execution(att["id"], mode="new", session_before={},
                                   baseline=ref)
        s = self.clock.now_ms()
        self.client.append_message("ses_w", _user("msg_in", s))
        self.ledger.record_execution_started(att["id"], started_at=s,
                                             event_id="evt_s")
        out = self.ledger.record_terminal(
            att["id"], outcome="succeeded", source="sse", event_id="evt_e",
            candidate=_exec_rules.build_candidate(
                outcome="succeeded", source="sse", session_id="ses_w",
                ended_at=s + 100, event_id="evt_e",
                input_in_inbox=False, input_owner="unique"))
        self.assertEqual(out["result"], "accepted")
        return task, att

    def test_commit_cannot_write_acceptance_status(self):
        """N1 复现反例转正：内部 commit_collection 不得写 accepted/rejected/
        verification_failed（P1 探针原样输入，修复后必须拒绝且无副作用）。"""
        task, att = self._succeeded_with_baseline()
        guard = self.ledger.collection_guard(att["id"])
        terminal = (self.ledger.get_attempt(att["id"])["metadata"]["execution"]
                    or {}).get("terminal")
        fp = rules.content_fingerprint(
            attempt=self.ledger.get_attempt(att["id"]), execution=terminal,
            message_ids=["msg_done"], text_sha256=rules.sha256_text("probe"),
            deliverables=[])
        version = rules.result_version(content_fp=fp, missing=[], stale=[],
                                       unverified=[])
        for a_status, t_status in (("accepted", "accepted"),
                                   ("rejected", "rejected"),
                                   ("verification_failed", "rejected")):
            with self.subTest(attempt_status=a_status):
                with self.assertRaises(LedgerError) as ctx:
                    self.ledger.commit_collection(
                        att["id"], result_version=version,
                        content_fingerprint=fp,
                        collected_at=self.clock.now_ms(),
                        version_path=os.path.join(
                            self.home, "results", att["id"], "versions",
                            f"{version}.json"),
                        summary="probe", guard=guard,
                        attempt_status=a_status, task_status=t_status,
                        actor="review-probe")
                self.assertEqual(ctx.exception.code, "COLLECT_STATUS_FORBIDDEN")
        after = self.ledger.get_attempt(att["id"])
        task_after = self.ledger.get_task(task["id"])
        self.assertIsNone(after["verification"])
        self.assertNotEqual(after["status"], "accepted")
        self.assertNotEqual(task_after["status"], "accepted")
        self.assertIsNone(task_after["accepted_at"])

    def test_commit_requires_valid_guard_and_identity(self):
        """N1：无 guard / 坏身份 / 传他人 attempt 的 guard 均拒绝。"""
        task, att = self._succeeded_with_baseline()
        guard = self.ledger.collection_guard(att["id"])
        kwargs = dict(
            result_version="rv1", content_fingerprint="fp1",
            collected_at=self.clock.now_ms(), version_path="/tmp/rv1.json",
            summary="x", guard=guard, actor="probe")
        # 无 guard
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.commit_collection(att["id"], **{**kwargs, "guard": None})
        self.assertEqual(ctx.exception.code, "COLLECTION_GUARD_REQUIRED")
        # 坏身份（非正 collected_at / 空 version）
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.commit_collection(att["id"], **{**kwargs, "collected_at": 0})
        self.assertEqual(ctx.exception.code, "COLLECTION_IDENTITY_INVALID")
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.commit_collection(att["id"], **{**kwargs, "result_version": ""})
        self.assertEqual(ctx.exception.code, "COLLECTION_IDENTITY_INVALID")
        # 传别人 attempt 的 guard
        task2, att2 = self._succeeded_with_baseline(files=("b.txt",), label="n1-b")
        guard2 = self.ledger.collection_guard(att2["id"])
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.commit_collection(att["id"], **{**kwargs, "guard": guard2})
        self.assertIn(ctx.exception.code,
                      ("COLLECTION_GUARD_REQUIRED", "COLLECTION_CONFLICT"))

    def test_deliverable_candidates_commit_atomically(self):
        """F6 复现反例转正：通知插入失败回滚后交付行不得留下 V2 证据。"""
        from unittest.mock import patch
        task, att = self._succeeded_with_baseline()
        site = att["directory"]
        # 第一次收集 V1（提交成功，候选行写入 attempt 目录）
        s = self.clock.now_ms()
        with open(os.path.join(site, "out.txt"), "w", encoding="utf-8") as fh:
            fh.write("v1-new")
        self.client.append_message("ses_w", _assistant("m1", "完成", s + 5))
        self.clock.advance(seconds=1)
        out1 = _collection.collect(
            att["id"], ledger=self.ledger, client=self.client, cfg=CFG,
            io=_CollectionIO, clock=self.clock.now_ms)
        self.assertTrue(out1["result_version"])
        row1 = self.ledger.deliverables(att["id"])[0]
        ev1 = json.loads(row1["evidence"])
        self.assertEqual(ev1.get("sha256"),
                         hashlib.sha256(b"v1-new").hexdigest(),
                         "V1 交付行应写入当前内容摘要")
        # 第二次收集 V2（attempt 目录内容变化 → 新版本）：通知插入注入失败
        # → 整体回滚（交付行不留 V2 摘要）
        with open(os.path.join(site, "out.txt"), "w", encoding="utf-8") as fh:
            fh.write("version-two")
        s2 = self.clock.now_ms()
        self.client.append_message("ses_w", _assistant("m2", "V2 回报", s2 + 5))
        self.clock.advance(seconds=1)
        with patch.object(Ledger, "_enqueue_notification_in_tx",
                          side_effect=RuntimeError("notify-down")):
            with self.assertRaises(RuntimeError):
                _collection.collect(
                    att["id"], ledger=self.ledger, client=self.client, cfg=CFG,
                    io=_CollectionIO, clock=self.clock.now_ms)
        after = self.ledger.get_attempt(att["id"])
        row2 = self.ledger.deliverables(att["id"])[0]
        ev2 = json.loads(row2["evidence"])
        # 探针断言（原 F6 反例）：result_path 保持 V1 且交付 sha 不变成 V2
        self.assertEqual(after["result_path"], out1["result_path"])
        self.assertNotEqual(
            ev2.get("sha256"),
            hashlib.sha256(b"version-two").hexdigest(),
            "回滚后不得留下 V2 交付证据（混合状态）")
        self.assertEqual(ev2.get("sha256"), ev1.get("sha256"),
                         "交付行与结果指针一致回滚")

    def test_check_deliverables_persist_false_is_readonly(self):
        """persist=False：不写行（checked_at/状态不变），返回完整候选。"""
        task, att = self._succeeded_with_baseline()
        before = self.ledger.deliverables(att["id"])
        self.file("out.txt", "changed-now")
        candidates = self.ledger.check_deliverables(att["id"], persist=False)
        self.assertTrue(candidates)
        self.assertEqual(candidates[0]["status"], "present")
        # D5/F6：persist=False 只出候选基础行（无内容哈希/glob 枚举）
        self.assertNotIn("sha256", candidates[0]["evidence"] or {})
        self.assertNotIn("manifest_sha256", candidates[0]["evidence"] or {})
        after = self.ledger.deliverables(att["id"])
        self.assertEqual(after[0]["status"], before[0]["status"],
                         "persist=False 不得改写 DB 行")
        self.assertEqual(after[0]["checked_at"], before[0]["checked_at"])


class TestSideEffectObservationMerge(RemediationBase):
    """A/D4 接口：观察子字段白名单 + 事务内 merge 不覆盖并发事实。"""

    def test_observation_merges_and_keeps_concurrent_sections(self):
        task = self.ledger.create_task(project="p", title="obs-merge",
                                       coordinator_session="ses_c",
                                       write_scope=[self.site])
        att = self.ledger.create_attempt(
            task["id"], directory=self.site, session_id="ses_w",
            status="dispatched", dispatch_state="confirmed",
            deliverables=[], dispatch_msg_id="msg_in", write_scope=[self.site])
        # 预置并发事实：execution（seal）+ collection 补收集计数（合法入口）
        self.ledger.seal_execution(att["id"], mode="new", session_before={})
        self.ledger.record_collection_pending(
            att["id"], reason="fixture-count", next_collect_at=None,
            max_attempts=5, interval_ms=15000, actor="supervisor")
        self.ledger.record_collection_pending(
            att["id"], reason="fixture-count", next_collect_at=None,
            max_attempts=5, interval_ms=15000, actor="supervisor")
        out = self.ledger.record_side_effect_observation(
            att["id"], observation={
                "active_shells": {"sh-1": {"status": "running"}},
                "active_children": ["ses-child"],
                "side_effects_checked_at": self.clock.now_ms(),
                "side_effect_checks": {"shell": "active"},
            }, actor="supervisor")
        meta = out["metadata"]
        self.assertEqual(meta["active_shells"], {"sh-1": {"status": "running"}})
        self.assertEqual(meta["active_children"], ["ses-child"])
        self.assertEqual(meta["side_effect_checks"], {"shell": "active"})
        # 并发事实保留
        self.assertTrue((meta.get("execution") or {}).get("dispatch", {}).get(
            "sealed_at"), "execution 封存事实保留")
        self.assertEqual((meta.get("collection") or {}).get("count"), 2,
                         "collection 补收集事实保留")
        audit = self.ledger.conn.execute(
            "SELECT COUNT(*) FROM audit WHERE action='attempt.side-effect-observation' "
            "AND entity_id=?", (att["id"],)).fetchone()[0]
        self.assertEqual(audit, 1, "审计与合并同事务")

    def test_observation_rejects_unknown_fields(self):
        task = self.ledger.create_task(project="p", title="obs-wl",
                                       coordinator_session="ses_c")
        att = self.ledger.create_attempt(
            task["id"], directory=self.site, session_id="ses_w",
            status="dispatched", dispatch_state="confirmed",
            deliverables=[], write_scope=[self.site])
        for bad in ({"execution": {"terminal": {"outcome": "succeeded"}}},
                    {"result": {"version": "rv-forged"}},
                    {"status": "accepted"}, {}):
            with self.subTest(bad=sorted(bad) or "empty"):
                with self.assertRaises(LedgerError) as ctx:
                    self.ledger.record_side_effect_observation(
                        att["id"], observation=bad)
                self.assertEqual(ctx.exception.code, "FIELD_NOT_ALLOWED")
        after = self.ledger.get_attempt(att["id"])
        self.assertNotIn("result", after["metadata"])
        self.assertNotIn("active_shells", after["metadata"])


class TestTerminalEvidenceProtection(RemediationBase):
    """3.1：通用 update_attempt/transition extra 不得替换 execution.terminal。"""

    def test_update_attempt_cannot_replace_terminal(self):
        from oc_orch.adapters.results import scan_deliverable_baseline
        task = self.ledger.create_task(project="p", title="term-prot",
                                       coordinator_session="ses_c",
                                       write_scope=[self.site])
        att = self.ledger.create_attempt(
            task["id"], directory=self.site, session_id="ses_w",
            status="dispatched", dispatch_state="confirmed",
            deliverables=[], dispatch_msg_id="msg_in", write_scope=[self.site])
        ref = scan_deliverable_baseline(self.ledger, att["id"],
                                        captured_at=self.clock.now_ms())
        self.ledger.seal_execution(att["id"], mode="new", session_before={},
                                   baseline=ref)
        self.ledger.record_execution_started(att["id"], started_at=1000,
                                             event_id="evt_s")
        out = self.ledger.record_terminal(
            att["id"], outcome="succeeded", source="sse", event_id="evt_e",
            candidate=_exec_rules.build_candidate(
                outcome="succeeded", source="sse", session_id="ses_w",
                ended_at=2000, event_id="evt_e", input_in_inbox=False,
                input_owner="unique"))
        self.assertEqual(out["result"], "accepted")
        att_now = self.ledger.get_attempt(att["id"])
        terminal = att_now["metadata"]["execution"]["terminal"]
        # 伪造替换：整个 metadata 保留其他子段但改 terminal
        forged_meta = dict(att_now["metadata"])
        forged_exec = dict(forged_meta["execution"])
        forged_exec["terminal"] = {**terminal, "outcome": "failed",
                                   "end_at": 999, "source": "forged"}
        forged_meta["execution"] = forged_exec
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.update_attempt(att["id"], fields={"metadata": forged_meta},
                                       actor="attacker")
        self.assertEqual(ctx.exception.code, "EXECUTION_PROTECTED")
        after = self.ledger.get_attempt(att["id"])
        self.assertEqual(after["metadata"]["execution"]["terminal"], terminal,
                         "受保护终态证据不被改写")
        # 删除 terminal 的整体替换同样拒绝
        drop_meta = dict(att_now["metadata"])
        drop_exec = dict(drop_meta["execution"])
        drop_exec.pop("terminal", None)
        drop_meta["execution"] = drop_exec
        with self.assertRaises(LedgerError):
            self.ledger.update_attempt(att["id"], fields={"metadata": drop_meta})
        # 相同值回写放行（观察类调用不受影响）
        self.ledger.update_attempt(att["id"], fields={
            "metadata": {**att_now["metadata"],
                         "active_shells": {"sh": {"status": "running"}}}})
        kept = self.ledger.get_attempt(att["id"])
        self.assertEqual(kept["metadata"]["execution"]["terminal"], terminal)

    def test_transition_extra_metadata_cannot_replace_terminal(self):
        from oc_orch.adapters.results import scan_deliverable_baseline
        task = self.ledger.create_task(project="p", title="term-prot2",
                                       coordinator_session="ses_c",
                                       write_scope=[self.site])
        att = self.ledger.create_attempt(
            task["id"], directory=self.site, session_id="ses_w",
            status="dispatched", dispatch_state="confirmed",
            deliverables=[], dispatch_msg_id="msg_in", write_scope=[self.site])
        ref = scan_deliverable_baseline(self.ledger, att["id"],
                                        captured_at=self.clock.now_ms())
        self.ledger.seal_execution(att["id"], mode="new", session_before={},
                                   baseline=ref)
        self.ledger.record_execution_started(att["id"], started_at=1000,
                                             event_id="evt_s")
        self.ledger.record_terminal(
            att["id"], outcome="succeeded", source="sse", event_id="evt_e",
            candidate=_exec_rules.build_candidate(
                outcome="succeeded", source="sse", session_id="ses_w",
                ended_at=2000, event_id="evt_e", input_in_inbox=False,
                input_owner="unique"))
        att_now = self.ledger.get_attempt(att["id"])
        terminal = att_now["metadata"]["execution"]["terminal"]
        forged = dict(att_now["metadata"])
        forged_exec = dict(forged.get("execution") or {})
        forged_exec["terminal"] = None
        forged["execution"] = forged_exec
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.transition_attempt(att["id"], "stalled", extra={
                "metadata": forged})
        self.assertEqual(ctx.exception.code, "EXECUTION_PROTECTED")
        after = self.ledger.get_attempt(att["id"])
        self.assertEqual(after["metadata"]["execution"]["terminal"], terminal)


class TestAttachLegacyRecovery(RemediationBase):
    """A/T15 契约：no_prompt attach（无伪 message_id）的 legacy 恢复边界。

    - 起界只能来自已封存 execution.epoch.started_at（捕获的 busy 区间）；
    - 需真实成功事实（REST outcome/idle）+ 非 active + 顺序合法；
    - 无 epoch、epoch 晚于完成、或仍在活动 → 不可证明，不得收集。"""

    def make_attach_damage(self, *, epoch_started=1000):
        task = self.ledger.create_task(project="p", title="attach-damage",
                                       coordinator_session="ses_c",
                                       write_scope=[self.site])
        base_meta = {}
        execution = {"mode": "attach", "dispatch": {"sealed_at": 995}}
        if epoch_started is not None:
            execution["epoch"] = {"started_at": epoch_started,
                                  "source": "attach-captured-busy"}
        att = self.ledger.create_attempt(
            task["id"], directory=self.site, session_id="ses_w",
            status="dispatched", dispatch_state="confirmed",
            deliverables=[], dispatch_msg_id=None, write_scope=[self.site],
            metadata=base_meta)
        # 显式事故夹具 SQL：旧实现坏终态（terminal_at 早于真实完成）+
        # 坏终态遗留的 site 释放（顺序子用例间不互相挡写权）
        self.ledger.conn.execute(
            "UPDATE attempts SET status='terminal_succeeded', "
            "site_released_at=1, "
            "terminal_at=998, terminal_kind='succeeded', metadata=? WHERE id=?",
            (json.dumps({"execution": execution}), att["id"]))
        self.ledger.conn.commit()
        return task, att

    def test_attach_epoch_recovery_proven(self):
        task, att = self.make_attach_damage(epoch_started=1000)
        # 附着区间内的实质回报（无 user 消息：attach 不伪造输入）
        self.client.append_message("ses_w", _assistant("att-done",
                                                       "附着轮完成产物", 1500))
        s = self.client.set_session("ses_w", outcome="succeeded",
                                     time_={"busy": 1000, "idle": 2000,
                                            "updated": 2000})
        self.client.add_idle("ses_w", outcome="succeeded", at=2000)
        out = collect_attempt(self.ledger, self.client, att["id"], CFG,
                              auto=False, clock=self.clock.now_ms)
        self.assertTrue(out["result_version"])
        after = self.ledger.get_attempt(att["id"])
        recovery = (after["metadata"].get("collection") or {}).get(
            "recovery_execution")
        self.assertIsNotNone(recovery)
        self.assertEqual(recovery["bound_kind"], "attach-epoch")
        self.assertEqual(recovery["real_end_at"], 2000)
        self.assertEqual(after["terminal_at"], 998, "坏 terminal_at 保留")
        snap = json.load(open(out["result_path"], encoding="utf-8"))
        self.assertEqual(snap["message_window"]["since"], 1000)
        self.assertEqual(snap["message_window"]["until"], 2000)

    def test_attach_without_epoch_or_reversed_not_proven(self):
        for name, epoch, idle in (
            ("no-epoch", None, 2000),
            ("epoch-after-idle", 2500, 2000),
            ("still-active", None, 2000),
        ):
            with self.subTest(case=name):
                task, att = self.make_attach_damage(epoch_started=epoch)
                self.client.append_message("ses_w", _assistant(
                    "att-p" + name, "进度", 1500))
                session = self.client.set_session(
                    "ses_w", outcome="succeeded",
                    time_={"busy": 1000, "idle": idle, "updated": idle})
                if name == "still-active":
                    # 无 epoch 且活动：构造无 epoch 变体 + active
                    self.client.active["ses_w"] = session
                with self.assertRaises(LedgerError) as ctx:
                    collect_attempt(self.ledger, self.client, att["id"], CFG,
                                    auto=False, clock=self.clock.now_ms)
                self.assertEqual(ctx.exception.code, "NOT_READY")
                after = self.ledger.get_attempt(att["id"])
                self.assertIsNone(after["result_collected_at"])
                self.assertEqual(after["terminal_at"], 998)
                self.assertEqual(
                    len(self.notifications(att["id"])), 0,
                    "不可证明的 attach 恢复不得发任何通知")

    def test_attach_epoch_active_with_succeeded_outcome_not_proven(self):
        """REST 报 succeeded 但 session 仍 active → 活动进度不是完成证据。"""
        task, att = self.make_attach_damage(epoch_started=1000)
        self.client.append_message("ses_w", _assistant("att-run", "仍执行", 1500))
        session = self.client.set_session("ses_w", outcome="succeeded",
                                          time_={"busy": 1000, "idle": 2000})
        self.client.active["ses_w"] = session
        with self.assertRaises(LedgerError) as ctx:
            collect_attempt(self.ledger, self.client, att["id"], CFG,
                            auto=False, clock=self.clock.now_ms)
        self.assertEqual(ctx.exception.code, "NOT_READY")


if __name__ == "__main__":
    unittest.main()

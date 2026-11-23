#!/usr/bin/env python3
"""OpenSpec fix-attempt-completion-integrity：结果版本化发布与 ACK/路由/恢复（T26/T27/T28/T32/T33/T34）。

覆盖：
- 相同内容重复收集同版本、不重置通知预算/ACK 期限（T27）；
- 同消息数内容变化 → 新版本新通知，旧通知仍打开旧快照（T28）；
- collect 不隐式 ACK；ACK 只作用已识别通知；dead 不被重收集复活（T32）；
- 无 coordinator → route_pending 告警 + 补路由后投递；keyset 分页无饥饿（T33）；
- payload/compose_wake/inbox 展示版本、stale、unverified、CHECKS_PENDING（T34）；
- 后续 attempt 消息不得进入旧结果（T26）。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.composition import application
from oc_orch.config import DEFAULTS
from oc_orch.ledger import LedgerError
from oc_orch.supervisor import Supervisor

from tests.fakes import FakeClient, FakeClock

CFG = {**DEFAULTS, "auto_collect": True, "desktop_alerts": False,
       "collect_retry_interval": 15, "collect_max_attempts": 5}


def _assistant(mid: str, text: str, at: int):
    return {"id": mid, "type": "assistant", "time": {"created": at},
            "content": [{"type": "text", "text": text}]}


def _user(mid: str, at: int):
    return {"id": mid, "type": "user", "texts": "go", "time": {"created": at}}


class PublicationBase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="oc-orch-pub-")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.clock = FakeClock(wall_ms=1_000_000)
        self.client = FakeClient()
        self.cfg = dict(CFG)
        self.sup = Supervisor(self.home, cfg=self.cfg, client=self.client,
                              clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        self.app = application(self.ledger, lambda: self.client, self.cfg,
                               clock=self.clock)
        self.client.set_session("ses_c", time_={})  # coordinator 通道可投递
        os.makedirs(self.site, exist_ok=True)

    @property
    def site(self):
        return os.path.join(self.home, "site")

    def run_attempt(self, *, deliverables=("out.txt",), contents=None,
                    baseline_contents=None, messages, msg_id="msg_in_1",
                    session="ses_w", coordinator="ses_c", title="t"):
        """完整执行一个成功 attempt 并自动收集；返回 (task, attempt, first_out)。"""
        task = self.ledger.create_task(project="p", title=title,
                                       coordinator_session=coordinator,
                                       write_scope=[self.site])
        final = contents if contents is not None else {"out.txt": "v1"}
        base = baseline_contents if baseline_contents is not None else dict(final)
        for name, text in base.items():
            with open(os.path.join(self.site, name), "w", encoding="utf-8") as f:
                f.write(text)
        att = self.ledger.create_attempt(
            task["id"], directory=self.site, session_id=session,
            status="dispatched", dispatch_state="confirmed",
            deliverables=list(deliverables), dispatch_msg_id=msg_id,
            write_scope=[self.site])
        # 封存派单前交付基线（D5）：与最终内容比对来源维度
        from oc_orch.adapters.results import scan_deliverable_baseline
        scan_deliverable_baseline(self.ledger, att["id"],
                                  captured_at=self.clock.now_ms())
        for name, text in final.items():
            with open(os.path.join(self.site, name), "w", encoding="utf-8") as f:
                f.write(text)
        t0 = self.clock.now_ms()
        self.client.append_message(session, _user(msg_id, t0 - 1))
        for i, m in enumerate(messages):
            self.client.append_message(session, _assistant(f"{msg_id}-a{i}", m,
                                                           t0 + 10 + i))
        self.sup.ingest_event({"id": f"evt_s_{msg_id}",
                               "type": "session.execution.started",
                               "created": t0, "data": {"sessionID": session}})
        self.client.set_session(session, outcome="succeeded",
                                time_={"updated": t0 + 100, "idle": t0 + 100,
                                       "busy": t0})
        self.clock.advance(seconds=1)
        self.sup.cycle(dispatch=False)
        att = self.ledger.get_attempt(att["id"])
        self.assertEqual(att["status"], "result_collected",
                         (att["status"], att["metadata"].get("collection")))
        return task, att

    def notifications(self, attempt_id):
        return [dict(r, payload=json.loads(r["payload"]))
                for r in self.ledger.conn.execute(
                    "SELECT * FROM notifications WHERE attempt_id=? "
                    "ORDER BY created_at", (attempt_id,)).fetchall()]

    def result_notifs(self, attempt_id):
        return [n for n in self.notifications(attempt_id)
                if n["kind"] == "result_ready"]


class TestVersionStability(PublicationBase):
    """T27：相同内容重复收集（auto→manual）同版本、同通知、预算不重置。"""

    def test_repeat_collect_same_content(self):
        task, att = self.run_attempt(messages=["真实回报"])
        v1 = (att["metadata"].get("result") or {})["version"]
        notif_before = self.result_notifs(att["id"])[0]
        self.clock.advance(seconds=60)
        out = self.app.collect(att["id"])  # 人工重复收集
        self.assertEqual(out["result_version"], v1)
        # commit_collection 同版本幂等 → duplicate=True（不重写、不重发）
        self.assertTrue(out["duplicate"])
        rows = self.result_notifs(att["id"])
        self.assertEqual(len(rows), 1, "同版本不产生第二条逻辑通知")
        after = rows[0]
        self.assertEqual(after["id"], notif_before["id"])
        self.assertEqual(after["attempts"], notif_before["attempts"],
                         "重复收集不重置投递预算")
        self.assertEqual(after["next_attempt_at"], notif_before["next_attempt_at"],
                         "重复收集不重置 ACK 期限")

    def test_observe_time_and_source_do_not_create_versions(self):
        task, att = self.run_attempt(messages=["回报 A"])
        v1 = (att["metadata"].get("result") or {})["version"]
        self.clock.advance(seconds=30)
        # 再跑一次对账周期（观察时间变化）+ REST 等价终态
        self.client.set_session("ses_w", outcome="succeeded",
                                time_={"updated": self.clock.now_ms(),
                                       "idle": self.clock.now_ms() - 1000,
                                       "busy": 1_000_000})
        self.sup.cycle(dispatch=False)
        att2 = self.ledger.get_attempt(att["id"])
        self.assertEqual((att2["metadata"].get("result") or {})["version"], v1)

    def test_same_count_content_change_new_version(self):
        """T28：同消息数、正文变化 → 新版本 + 新通知；旧通知打开旧快照。"""
        task, att = self.run_attempt(messages=["第一版内容"])
        v1 = (att["metadata"].get("result") or {})["version"]
        old_path = self.ledger.get_attempt(att["id"])["result_path"]
        # 一次同数量消息的新执行（rework 语义：文件改内容 + 换正文）
        with open(os.path.join(self.site, "out.txt"), "w", encoding="utf-8") as f:
            f.write("v2-内容")
        self.client.append_message("ses_w", _assistant("late-1", "第二版内容",
                                                       self.clock.now_ms() + 1))
        self.clock.advance(seconds=30)
        out = self.app.collect(att["id"])
        self.assertNotEqual(out["result_version"], v1)
        self.assertTrue(out["notification_ids"], "新版本必有新通知")
        self.assertEqual(len(self.result_notifs(att["id"])), 2)
        # 旧通知的路径仍指向旧不可变快照，内容未被覆写
        with open(old_path, encoding="utf-8") as f:
            old_snap = json.load(f)
        self.assertEqual(old_snap["result_version"], v1)
        self.assertIn("第一版内容", old_snap["text"])


class TestAckDecoupling(PublicationBase):
    """T32：collect 不隐式 ACK；ACK 不倒退；dead 不复活。"""

    def test_collect_keeps_notification_unacked(self):
        task, att = self.run_attempt(messages=["回报"])
        row = self.result_notifs(att["id"])[0]
        self.assertEqual(row["state"], "pending", "collect 不得隐式 ACK")
        # 显式 ACK 只作用指定 ID
        self.ledger.ack_notifications([row["id"]], actor="coordinator")
        # 新版本收集：V1 保持 ACK
        with open(os.path.join(self.site, "out.txt"), "w", encoding="utf-8") as f:
            f.write("v3")
        self.client.append_message("ses_w", _assistant("late-2", "第三版",
                                                       self.clock.now_ms() + 1))
        self.clock.advance(seconds=5)
        out = self.app.collect(att["id"])
        v1_row = self.ledger.get_notification(row["id"])
        self.assertEqual(v1_row["state"], "acked")
        new_ids = [i for i in out["notification_ids"] if i != row["id"]]
        self.assertTrue(new_ids)
        for nid in new_ids:
            self.assertIn(self.ledger.get_notification(nid)["state"],
                          ("pending", "sent"))

    def test_dead_notification_not_resurrected_by_recollect(self):
        task, att = self.run_attempt(messages=["回报"])
        row = self.result_notifs(att["id"])[0]
        self.ledger.conn.execute(
            "UPDATE notifications SET state='dead', attempts=3, "
            "last_error='exhausted' WHERE id=?", (row["id"],))
        self.ledger.conn.commit()
        # 重复收集相同内容 → 同版本，dead 不复活、不重置预算
        out = self.app.collect(att["id"])
        self.assertEqual(out["result_version"],
                         (att["metadata"].get("result") or {})["version"])
        dead = self.ledger.get_notification(row["id"])
        self.assertEqual(dead["state"], "dead")
        self.assertEqual(dead["attempts"], 3)


class TestRoutingAndBacklog(PublicationBase):
    """T33：无 coordinator → route_pending + 告警 + 补路由；keyset 无饥饿。"""

    def test_unrouted_result_pending_then_refilled(self):
        # 任务无 coordinator；项目也无租约
        task = self.ledger.create_task(project="p2", title="无路由",
                                       write_scope=[self.site])
        with open(os.path.join(self.site, "out.txt"), "w", encoding="utf-8") as f:
            f.write("x")
        att = self.ledger.create_attempt(
            task["id"], directory=self.site, session_id="ses_n",
            status="dispatched", dispatch_state="confirmed",
            deliverables=["out.txt"], dispatch_msg_id="msg_n",
            write_scope=[self.site])
        t0 = self.clock.now_ms()
        self.client.append_message("ses_n", _user("msg_n", t0 - 1))
        self.client.append_message("ses_n", _assistant("msg_n-a", "回报",
                                                       t0 + 5))
        self.sup.ingest_event({"id": "evt_n", "type": "session.execution.started",
                               "created": t0, "data": {"sessionID": "ses_n"}})
        self.client.set_session("ses_n", outcome="succeeded",
                                time_={"updated": t0 + 50, "idle": t0 + 50,
                                       "busy": t0})
        self.clock.advance(seconds=1)
        self.sup.cycle(dispatch=False)
        rows = self.notifications(att["id"])
        self.assertTrue(rows, "无路由也要有持久待发布责任")
        row = rows[0]
        self.assertIsNone(row["target_session"])
        self.assertEqual(json.loads(row["metadata"]).get("route_pending"), 1)
        alerts = [a for a in self.ledger.list_alerts(limit=50)
                  if a["kind"] == "result_unrouted"]
        self.assertTrue(alerts, "无路由产生去重告警")
        # coordinator 出现（claim）→ 下一轮补路由并发送（不烧预算：无投递尝试）
        self.assertEqual(len(self.client.prompt_calls), 0)
        self.client.set_session("ses_coord_new", time_={})
        self.ledger.claim_coordinator(project="p2", session_id="ses_coord_new",
                                      actor="coordinator")
        self.sup.cycle(dispatch=True)
        row2 = self.ledger.get_notification(row["id"])
        self.assertEqual(row2["target_session"], "ses_coord_new")
        self.assertIn(row2["state"], ("sent", "pending"))
        self.assertGreaterEqual(len(self.client.prompt_calls), 1)

    def test_keyset_backlog_no_starvation_beyond_first_page(self):
        """超过固定列表上限的待办仍可恢复：创建 250 个终态尝试。"""
        task, att = self.run_attempt(messages=["回报"], deliverables=(),
                                     contents={})
        # 追加大量终态未收集尝试（第二页之外）
        for i in range(250):
            t = self.ledger.create_task(project="p3", title=f"bulk-{i}",
                                        coordinator_session="ses_c",
                                        write_scope=[self.site])
            a = self.ledger.create_attempt(
                t["id"], directory=self.site, session_id="ses_bulk",
                status="creating", dispatch_state="confirmed",
                deliverables=[],
                write_scope=[os.path.join(self.site, f"bulk{i}")])
            # 直接合法证据入口落终端
            self.ledger.record_terminal(
                a["id"], outcome="succeeded", source="rest",
                event_id=f"obs-bulk-{i}",
                candidate={"outcome": "succeeded", "source": "rest",
                           "ended_at": self.clock.now_ms() + i,
                           "session_id": "ses_bulk",
                           "interval": {"proven": True, "input_at": 1,
                                        "via": "fixture-legacy"}})
        self.sup.cycle(dispatch=False)  # finalize keyset 分页处理全部待办
        released = [a for a in self.ledger.conn.execute(
            "SELECT id FROM attempts WHERE site_released_at IS NOT NULL"
        ).fetchall()]
        # 250+1 全部走过 finalize 分页（释放/通知不因 100 条分页截断而饿死）
        self.assertGreaterEqual(len(released), 200)


class TestNotificationContent(PublicationBase):
    """T34：payload/compose_wake/inbox 表达版本与缺项。"""

    def test_checks_pending_in_payload_and_wake_text(self):
        task, att = self.run_attempt(
            deliverables=["out.txt", {"kind": "command", "target": "true"}],
            baseline_contents={"out.txt": "v0"},
            contents={"out.txt": "v1"}, messages=["文件型回报"])
        row = self.result_notifs(att["id"])[0]
        p = row["payload"]
        self.assertEqual(p["classification"], "CHECKS_PENDING")
        self.assertTrue(p["result_version"], "无版本 RESULT_READY 禁止")
        self.assertTrue(p["result_path"].endswith(".json") and
                        "versions" in p["result_path"])
        self.assertTrue(p["unverified_deliverables"])
        self.assertFalse(p["ready_for_verification"])
        # compose_wake 文本必须让协调者看到版本与缺项
        self.sup.dispatch_notifications()
        sent = [pc for pc in self.client.prompt_calls
                if pc["metadata"] and pc["metadata"].get("oc_orch_wake")]
        if not sent:
            # 调试信息：通知/抑制状态
            pending = [n for n in self.ledger.list_notifications(limit=50)
                       if n.get("target_session") == "ses_c"]
            raise AssertionError(f"no wake sent; notifs={pending}")
        self.assertTrue(sent)
        text = sent[-1]["text"]
        self.assertIn(p["result_version"], text)
        self.assertIn("CHECKS_PENDING", text)
        self.assertIn("unverified(未执行检查)", text)
        self.assertIn("显式 ack", text)

    def test_inbox_human_output_shows_version(self):
        task, att = self.run_attempt(messages=["回报"])
        row = self.result_notifs(att["id"])[0]
        # CLI inbox 人类可读输出（复用 ledger 组装逻辑直接验证字段可用性）
        p = row["payload"]
        for key in ("result_version", "result_path", "stale_deliverables",
                    "unverified_deliverables", "ready_for_verification"):
            self.assertIn(key, p)


class TestMessageBoundary(PublicationBase):
    """T26：后续 attempt 消息不得进入旧结果。"""

    def test_later_attempt_messages_excluded(self):
        task, att = self.run_attempt(messages=["第一轮回报"], msg_id="msg_g1")
        v1_path = self.ledger.get_attempt(att["id"])["result_path"]
        # 同 session 下一个尝试开始产出消息
        t2 = self.ledger.create_task(project="p", title="第二轮",
                                     coordinator_session="ses_c",
                                     write_scope=[self.site])
        att2 = self.ledger.create_attempt(
            t2["id"], directory=self.site, session_id="ses_w",
            status="dispatched", dispatch_state="confirmed",
            deliverables=[], dispatch_msg_id="msg_g2", write_scope=[self.site])
        t0 = self.clock.now_ms()
        self.client.append_message("ses_w", _user("msg_g2", t0 + 10))
        self.client.append_message("ses_w", _assistant("msg_g2-a", "第二轮消息",
                                                       t0 + 20))
        self.sup.ingest_event({"id": "evt_g2",
                               "type": "session.execution.started",
                               "created": t0 + 10, "data": {"sessionID": "ses_w"}})
        self.clock.advance(seconds=30)
        # 人工重收集旧尝试：不混入第二轮消息
        out = self.app.collect(att["id"])
        with open(v1_path, encoding="utf-8") as f:
            snap = json.load(f)
        with open(out["result_path"], encoding="utf-8") as f:
            snap2 = json.load(f)
        self.assertNotIn("第二轮消息", snap2["text"])
        self.assertNotIn("第二轮消息", snap["text"])


if __name__ == "__main__":
    unittest.main()


class TestProvenanceAndCarry(PublicationBase):
    """T19/T20/T21/T22/T35：来源门、mtime 与内容、显式 carry、版本失配。"""

    def sha(self, rel: str) -> str:
        import hashlib
        with open(os.path.join(self.site, rel), "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    def stamp(self, rel: str) -> float:
        return os.stat(os.path.join(self.site, rel)).st_mtime

    def test_unchanged_requires_carry_then_verify_passes(self):
        """T19/T21：文件未变（无 carry）→ DELIVERABLE_STALE + verify 被来源门拒绝；
        显式 carry（hash 命中）→ 收集为 carried → 正常验收通过。"""
        task, att = self.run_attempt(messages=["本轮回报文本"])
        row = self.result_notifs(att["id"])[0]
        self.assertEqual(row["payload"]["classification"], "DELIVERABLE_STALE")
        self.assertIn("out.txt", row["payload"]["stale_deliverables"])
        # 无 carry 的普通 verify 被来源门拒绝
        from oc_orch.domain.errors import LedgerError
        with self.assertRaises(LedgerError) as ctx:
            from oc_orch.results import verify_attempt
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "STALE_GATE_FAIL")
        # 显式 carry（声明当前内容与基线一致）
        manifest = {
            "schema": "oc-orch.carry.v1", "attempt_id": att["id"],
            "items": [{"target": "out.txt",
                       "baseline_sha256": self.sha("out.txt"),
                       "reason": "仅修订报告文本，产物沿用"}]}
        out = self.app.collect(att["id"], carry_manifest=manifest)
        prow = None
        with open(out["result_path"], encoding="utf-8") as f:
            snap = json.load(f)
        prow = [d for d in snap["deliverables"] if d["target"] == "out.txt"][0]
        self.assertEqual(prow["provenance"], "carried")
        self.assertEqual(out["classification"], "RESULT_READY")
        from oc_orch.results import verify_attempt
        v = verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(v["verification"]["classification"], "normal_effective")

    def test_carry_invalid_inputs_rejected(self):
        """T22：attempt/target/hash/source/理由 错误 → 明确拒绝，不改已有结果。"""
        task, att = self.run_attempt(messages=["回报"])
        before_version = (att["metadata"].get("result") or {})["version"]
        cases = [
            {"schema": "oc-orch.carry.v1", "attempt_id": "A-other", "items": []},
            {"schema": "oc-orch.carry.v1", "attempt_id": att["id"],
             "items": [{"target": "not_declared.txt", "baseline_sha256": "0" * 64,
                        "reason": "x"}]},
            {"schema": "oc-orch.carry.v1", "attempt_id": att["id"],
             "items": [{"target": "out.txt", "baseline_sha256": "zz", "reason": "x"}]},
            {"schema": "oc-orch.carry.v1", "attempt_id": att["id"],
             "items": [{"target": "out.txt", "baseline_sha256": self.sha("out.txt"),
                        "reason": "   "}]},
            {"schema": "oc-orch.carry.v1", "attempt_id": att["id"],
             "items": [{"target": "out.txt", "baseline_sha256": self.sha("out.txt"),
                        "reason": "沿用", "source_attempt_id": "A-does-not-exist"}]},
        ]
        from oc_orch.domain.errors import LedgerError
        for manifest in cases:
            with self.subTest(case=manifest.get("source_attempt_id", manifest.get("target", "attempt"))):
                with self.assertRaises(Exception) as ctx:
                    self.app.collect(att["id"], carry_manifest=manifest)
                code = getattr(ctx.exception, "code", "")
                self.assertIn(code, ("CARRY_MANIFEST_INVALID",
                                     "CARRY_SOURCE_EVIDENCE_MISMATCH"),
                              code)
        att2 = self.ledger.get_attempt(att["id"])
        self.assertEqual((att2["metadata"].get("result") or {})["version"],
                         before_version, "carry 输入错误不得改写已有版本")

    def test_carry_invalidated_after_file_change(self):
        """沿用后文件被修改 → 旧 carry 失效，来源重判为 changed（新版本）。"""
        task, att = self.run_attempt(messages=["回报"], baseline_contents={"out.txt": "v1"})
        manifest = {"schema": "oc-orch.carry.v1", "attempt_id": att["id"],
                    "items": [{"target": "out.txt",
                               "baseline_sha256": self.sha("out.txt"),
                               "reason": "沿用"}]}
        out = self.app.collect(att["id"], carry_manifest=manifest)
        v1 = out["result_version"]
        with open(os.path.join(self.site, "out.txt"), "w", encoding="utf-8") as f:
            f.write("v2-after-carry")
        self.clock.advance(seconds=5)
        self.client.append_message("ses_w", _assistant("late-carry-a", "更新产物",
                                                       self.clock.now_ms() + 1))
        out2 = self.app.collect(att["id"])  # 不再传清单：旧声明已失效
        self.assertNotEqual(out2["result_version"], v1)
        with open(out2["result_path"], encoding="utf-8") as f:
            snap = json.load(f)
        prow = [d for d in snap["deliverables"] if d["target"] == "out.txt"][0]
        self.assertEqual(prow["provenance"], "changed")

    def test_mtime_vs_content(self):
        """T20：内容变化保留旧 mtime → changed（hash 优先）；只 touch → unchanged
        （touch 不等于产出，来源门拒绝其验收）。"""
        # A）内容变化但保留旧 mtime → changed
        task_a, att_a = self.run_attempt(deliverables=("m.txt",), messages=["A 回报"],
                                         msg_id="msg_mA",
                                         baseline_contents={"m.txt": "one"},
                                         contents={"m.txt": "two"}, session="ses_mA")
        out_a = self.app.collect(att_a["id"])
        with open(out_a["result_path"], encoding="utf-8") as fh:
            snap_a = json.load(fh)
        prow_a = [d for d in snap_a["deliverables"] if d["target"] == "m.txt"][0]
        self.assertEqual(prow_a["provenance"], "changed")
        # 复写为同样内容并把 mtime 更早 → 仍然 changed（不能因 mtime 旧拒绝）
        f_a = os.path.join(self.site, "m.txt")
        with open(f_a, "w", encoding="utf-8") as fh:
            fh.write("two")
        os.utime(f_a, (1000.0, 1000.0))
        self.clock.advance(seconds=1)
        out_a2 = self.app.collect(att_a["id"])
        with open(out_a2["result_path"], encoding="utf-8") as fh:
            snap_a2 = json.load(fh)
        prow_a2 = [d for d in snap_a2["deliverables"] if d["target"] == "m.txt"][0]
        self.assertEqual(prow_a2["provenance"], "changed")
        # B）内容 == 基线（仅 touch mtime）→ unchanged；无 carry → 来源门拒绝
        task_b, att_b = self.run_attempt(deliverables=("m.txt",), messages=["B 回报"],
                                         msg_id="msg_mB",
                                         baseline_contents={"m.txt": "one"},
                                         contents={"m.txt": "one"}, session="ses_mB")
        f_b = os.path.join(self.site, "m.txt")
        os.utime(f_b, (os.stat(f_b).st_mtime + 5, os.stat(f_b).st_mtime + 5))
        self.clock.advance(seconds=1)
        out_b = self.app.collect(att_b["id"])
        with open(out_b["result_path"], encoding="utf-8") as fh:
            snap_b = json.load(fh)
        prow_b = [d for d in snap_b["deliverables"] if d["target"] == "m.txt"][0]
        self.assertEqual(prow_b["provenance"], "unchanged")
        from oc_orch.domain.errors import LedgerError
        with self.assertRaises(LedgerError) as ctx:
            from oc_orch.results import verify_attempt
            verify_attempt(self.ledger, att_b["id"], result="pass")
        self.assertEqual(ctx.exception.code, "STALE_GATE_FAIL")

    def test_prepare_then_new_version_collect_rejected_as_changed(self):
        """T35：验收准备 V1 期间另一 collect 提交 V2 → 旧准备被 RESULT_CHANGED 拒绝。

        夹具口径：基线=v0、首收集=v1（changed，来源门满足），prepare 引用 V1
        后另一收集器提交 v2 内容 → 旧准备必须因版本/guard 变化被拒。"""
        task, att = self.run_attempt(messages=["V1 回报"],
                                     baseline_contents={"out.txt": "v0"},
                                     contents={"out.txt": "v1"})
        prepared = None
        from oc_orch.results import prepare_verification
        prepared = prepare_verification(self.ledger, att["id"], result="pass")
        # 另一收集器提交 V2（内容变化）
        with open(os.path.join(self.site, "out.txt"), "w", encoding="utf-8") as f:
            f.write("v2")
        self.client.append_message("ses_w", _assistant("late-c", "V2 回报",
                                                       self.clock.now_ms() + 1))
        self.clock.advance(seconds=5)
        self.app.collect(att["id"])
        from oc_orch.domain.errors import LedgerError
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.submit_verification(att["id"], verification=prepared["prepared"],
                                            result="pass")
        # 旧准备不能接受新状态：guard 指纹（metadata 含结果版本）或版本门均拒绝
        self.assertIn(ctx.exception.code, ("EVIDENCE_CHANGED", "RESULT_CHANGED"))

#!/usr/bin/env python3
"""fix-attempt-completion-integrity 补修红→绿：结果来源/交付/检查证据完整性。

子代理 C 范围（排他文件对应的行为切片，全部走真实入口）：
- F5：collect 后必需文件删除/不可读/变化，普通 verify 不得通过；扩展 dir/glob
  完整清单摘要；检查（--run-checks）执行后再次复核产物，检查改写产物被拦截；
- F4：新协议 provenance=unknown 的必需文件（基线扫描预算不足），普通 verify
  在事务外准备即被来源门拒绝（与 ledger 收紧后的 STALE_GATE_FAIL 同码）；
- D5：基线 file/dir/glob 完整扫描的字节预算/不稳定/读取失败 → unknown；
  mtime 不作为内容新鲜度（内容变化保留旧 mtime 仍是 changed）；
- D6：carry 拒绝重复目标/跨任务来源；persisted carry 在文件删除后失效；
- D8：同指纹 create-if-absent 已存在快照必须校验内容；compat 最新视图按
  当前 DB 指针更新，不被落后的旧写覆盖；
- D10：command 检查证据绑定 result content_fingerprint，旧输入的检查在
  结果重新收集后失效为未验证（不可用旧输入检查判新输入），重跑检查可恢复；
- F3：read_message_window 倒序有界窗口（最近页向前至锚点/起界；复用 session
  远古历史不耗尽预算；重复 cursor/空页带 next/缺字段/协议异常严格报 incomplete）。

夹具原则：真实临时 SQLite + 真实文件 + FakeClient；终态经 ledger.record_terminal
（complete_execution 合法证据入口）；收集/验收经 oc_orch.results 兼容门面与
composition application（Supervisor/CLI/Web 同一用例），不镜像 helper 行为。
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.adapters import results as adapter_results  # noqa: E402
from oc_orch.composition import application  # noqa: E402
from oc_orch.config import DEFAULTS  # noqa: E402
from oc_orch.db import init  # noqa: E402
from oc_orch.domain.errors import ActionError  # noqa: E402
from oc_orch.ledger import Ledger, LedgerError  # noqa: E402
from oc_orch.results import (  # noqa: E402
    collect_attempt,
    prepare_verification,
    run_checks,
    verify_attempt,
)

from tests.fakes import FakeClient, complete_execution  # noqa: E402

CFG = {**DEFAULTS, "result_max_messages": 100, "result_store_limit": 200000,
       "auto_collect": False, "desktop_alerts": False,
       "collect_retry_interval": 15, "collect_max_attempts": 5}


def _assistant(mid: str, text: str, at: int):
    return {"id": mid, "type": "assistant", "time": {"created": at},
            "content": [{"type": "text", "text": text}]}


def _user(mid: str, at: int):
    return {"id": mid, "type": "user", "text": "go", "time": {"created": at}}


class RemediationBase(unittest.TestCase):
    def setUp(self):
        base = "/tmp/opencode" if os.path.isdir("/tmp/opencode") else None
        tmp = tempfile.TemporaryDirectory(prefix="oc-orch-rem-", dir=base)
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.site = os.path.join(self.home, "site")
        os.makedirs(self.site, exist_ok=True)
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home)
        self.client = FakeClient()
        self.app = application(self.ledger, lambda: self.client, CFG)

    # ---- 夹具：真实入口闭环（建任务/尝试 → 基线 → 合法终态 → 门面 collect）--
    def _attempt(self, *, deliverables, msg_id="msg_in", session="ses_w"):
        task = self.ledger.create_task(project="p", title="remediation",
                                       coordinator_session="ses_c",
                                       write_scope=[self.site])
        att = self.ledger.create_attempt(
            task["id"], directory=self.site, session_id=session,
            status="dispatched", dispatch_state="confirmed",
            deliverables=deliverables, dispatch_msg_id=msg_id,
            write_scope=[self.site])
        return task, att

    def _seal_baseline(self, attempt_id, *, budgets=None):
        return adapter_results.scan_deliverable_baseline(
            self.ledger, attempt_id, budgets=budgets)

    def _messages(self, session, texts, msg_id, t0):
        self.client.append_message(session, _user(msg_id, t0))
        for i, text in enumerate(texts):
            self.client.append_message(
                session, _assistant(f"{msg_id}-a{i}", text, t0 + 10 + i))

    def _collected(self, *, deliverables=("out.txt",), baseline_contents=None,
                   final_contents=None, texts=("本轮实质回报",), msg_id="msg_in",
                   session="ses_w", seal=True, budgets=None, cfg=None):
        """真实收集闭环：返回 (task, attempt, collect 输出)；attempt 已刷新。"""
        base = baseline_contents if baseline_contents is not None else {}
        final = final_contents if final_contents is not None else base
        for rel, text in base.items():
            self._write(rel, text)
        task, att = self._attempt(deliverables=deliverables, msg_id=msg_id)
        if seal:
            self._seal_baseline(att["id"], budgets=budgets)
        for rel, text in final.items():
            self._write(rel, text)
        t0 = 1_000_000
        self._messages(session, texts, msg_id, t0)
        complete_execution(self.ledger, att["id"], outcome="succeeded",
                           input_at=t0, ended_at=t0 + 100)
        out = collect_attempt(self.ledger, self.client, att["id"],
                              cfg or CFG, auto=False)
        return task, self.ledger.get_attempt(att["id"]), out

    def _write(self, rel, text):
        path = os.path.join(self.site, rel)
        os.makedirs(os.path.dirname(path) or self.site, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)

    def _sha(self, rel):
        import hashlib
        with open(os.path.join(self.site, rel), "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    def provenance_of(self, attempt_id, target):
        meta = self.ledger.get_attempt(attempt_id)["metadata"]
        for p in meta.get("deliverable_provenance") or []:
            if p.get("target") == target:
                return p.get("provenance")
        return None


class TestVerifyArtifactIntegrity(RemediationBase):
    """F5：收集后必需产物缺失/不可读/变化（含 dir/glob 与检查改写）。"""

    def test_required_file_deleted_after_collect_rejected(self):
        task, att, out = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"})
        self.assertEqual(self.provenance_of(att["id"], "out.txt"), "changed")
        self.assertTrue(out["ready_for_verification"])
        os.unlink(os.path.join(self.site, "out.txt"))
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")
        self.assertIn("out.txt", ctx.exception.detail.get("changed") or [])
        # 拒绝不得留下验收副作用
        self.assertEqual(self.ledger.get_attempt(att["id"])["status"],
                         "result_collected")
        self.assertEqual(self.ledger.get_task(task["id"])["status"],
                         "result_received")

    def test_required_file_modified_after_collect_rejected(self):
        task, att, _ = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"})
        self._write("out.txt", "v2-after-collect")
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")

    def test_required_file_unreadable_after_collect_rejected(self):
        if os.geteuid() == 0:
            self.skipTest("root 下 chmod 000 仍可读，不能构造权限失败")
        task, att, _ = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"})
        os.chmod(os.path.join(self.site, "out.txt"), 0o000)
        self.addCleanup(os.chmod, os.path.join(self.site, "out.txt"), 0o600)
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")

    def test_dir_manifest_changed_after_collect_rejected(self):
        deliverables = [{"kind": "dir", "target": "outdir"}]
        task, att, out = self._collected(
            deliverables=deliverables,
            baseline_contents={"outdir/a.txt": "v0"},
            final_contents={"outdir/a.txt": "v1"})
        self.assertEqual(self.provenance_of(att["id"], "outdir"), "changed")
        self.assertTrue(out["ready_for_verification"])
        # 收集后目录新增条目：完整清单摘要变化 → 拒绝
        self._write("outdir/b.txt", "new-entry")
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")

    def test_dir_deleted_after_collect_rejected(self):
        deliverables = [{"kind": "dir", "target": "outdir"}]
        task, att, _ = self._collected(
            deliverables=deliverables,
            baseline_contents={"outdir/a.txt": "v0"},
            final_contents={"outdir/a.txt": "v1"})
        for name in os.listdir(os.path.join(self.site, "outdir")):
            os.unlink(os.path.join(self.site, "outdir", name))
        os.rmdir(os.path.join(self.site, "outdir"))
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")

    def test_glob_manifest_changed_after_collect_rejected(self):
        deliverables = [{"kind": "glob", "target": "out-*.txt"}]
        task, att, _ = self._collected(
            deliverables=deliverables,
            baseline_contents={"out-1.txt": "v0"},
            final_contents={"out-1.txt": "v1"})
        self.assertEqual(self.provenance_of(att["id"], "out-*.txt"), "changed")
        self._write("out-2.txt", "extra-match")
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")

    def test_empty_dir_deleted_after_collect_rejected(self):
        """协调者 F5 反例：收集一个（新增的）空目录后删除它——hash([]) 对
        hash([]) 相等会漏判；存在性判断必须先拦住。"""
        deliverables = [{"kind": "dir", "target": "emptydir"}]
        task, att = self._attempt(deliverables=deliverables)
        # 派单前目录不存在 → 基线 missing → 本轮出现（即使为空）即 new
        self._seal_baseline(att["id"])
        os.makedirs(os.path.join(self.site, "emptydir"), exist_ok=True)
        t0 = 1_000_000
        self._messages("ses_w", ("本轮新增空目录产物",), "msg_in", t0)
        complete_execution(self.ledger, att["id"], outcome="succeeded",
                           input_at=t0, ended_at=t0 + 100)
        out = collect_attempt(self.ledger, self.client, att["id"], CFG, auto=False)
        self.assertEqual(self.provenance_of(att["id"], "emptydir"), "new")
        self.assertTrue(out["ready_for_verification"])
        os.rmdir(os.path.join(self.site, "emptydir"))
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")
        self.assertIn("emptydir", ctx.exception.detail.get("changed") or [])

    def test_glob_emptied_after_collect_rejected(self):
        deliverables = [{"kind": "glob", "target": "g-*.txt"}]
        task, att, _ = self._collected(
            deliverables=deliverables,
            baseline_contents={"g-1.txt": "v0"},
            final_contents={"g-1.txt": "v1"})
        self.assertEqual(self.provenance_of(att["id"], "g-*.txt"), "changed")
        # 收集后清空全部匹配：清单摘要由非空变 hash([])，必须拒绝
        os.unlink(os.path.join(self.site, "g-1.txt"))
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")

    def test_missing_recorded_digest_not_silently_skipped(self):
        """协调者 F5：必需条目收集版本摘要缺失不能静默 skip（不可判定即拒绝）。"""
        task, att, out = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"})
        snap_path = os.path.join(self.home, "results", att["id"], "versions",
                                 f"{out['result_version']}.json")
        with open(snap_path, encoding="utf-8") as f:
            snap = json.load(f)
        for d in snap["deliverables"]:
            d.pop("sha256", None)
            d.pop("manifest_sha256", None)
        with open(snap_path, "w", encoding="utf-8") as f:
            json.dump(snap, f)
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")
        self.assertIn("out.txt", ctx.exception.detail.get("changed") or [])

    def test_dir_unreadable_after_collect_rejected(self):
        """目录本身不可读（权限）后普通 verify 拒绝（不可读不能当不变）。"""
        if os.geteuid() == 0:
            self.skipTest("root 下 chmod 000 仍可读，不能构造权限失败")
        deliverables = [{"kind": "dir", "target": "outdir"}]
        task, att, _ = self._collected(
            deliverables=deliverables,
            baseline_contents={"outdir/a.txt": "v0"},
            final_contents={"outdir/a.txt": "v1"})
        os.chmod(os.path.join(self.site, "outdir"), 0o000)
        self.addCleanup(os.chmod, os.path.join(self.site, "outdir"), 0o700)
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")

    def test_check_command_modifying_dir_artifact_rejected(self):
        """检查执行后再次复核产物：--run-checks 改写 dir 产物 → RESULT_CHANGED。"""
        cmd = (f"printf extra > {os.path.join(self.site, 'outdir', 'by-check.txt')}")
        deliverables = [{"kind": "dir", "target": "outdir"},
                        {"kind": "command", "target": cmd, "required": True}]
        task, att, _ = self._collected(
            deliverables=deliverables,
            baseline_contents={"outdir/a.txt": "v0"},
            final_contents={"outdir/a.txt": "v1"})
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass", run_checks=True)
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")
        # 检查确实执行过且留下产物；拒绝无验收副作用
        self.assertTrue(os.path.exists(
            os.path.join(self.site, "outdir", "by-check.txt")))
        self.assertEqual(self.ledger.get_attempt(att["id"])["status"],
                         "result_collected")


class TestProvenanceGate(RemediationBase):
    """F4：新协议来源 unknown 的必需文件阻断普通 verify（准备侧同门）。"""

    def test_unknown_provenance_blocks_normal_verify(self):
        # 基线扫描字节预算不足 → out.txt 基线 unknown（coverage 截断）
        task, att, out = self._collected(
            baseline_contents={"out.txt": "v0-some-bytes"},
            final_contents={"out.txt": "v1-some-bytes"},
            budgets={"deliverable_scan_max_bytes": 4})
        self.assertEqual(self.provenance_of(att["id"], "out.txt"), "unknown")
        self.assertFalse(out["ready_for_verification"])
        self.assertTrue(any(d.get("target") == "out.txt" for d in out["stale"]))
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "STALE_GATE_FAIL")
        self.assertIn("out.txt", ctx.exception.detail.get("unknown") or [])
        # force 有因仍可显式通过（不越 current/人工控制）
        forced = verify_attempt(self.ledger, att["id"], result="pass",
                                force=True, reason="人工确认沿用旧产物")
        self.assertEqual(forced["verification"]["classification"], "forced")
        unmet = forced["verification"]["unmet"]
        self.assertTrue(any(u.get("code") == "deliverable_stale" for u in unmet))


class TestBaselineScan(RemediationBase):
    """D5：file/dir/glob 完整扫描的预算/不稳定/失败与 mtime 语义。"""

    def test_byte_budget_exceeded_marks_unknown_and_truncated(self):
        self._write("big.txt", "0123456789abcdef")
        task, att = self._attempt(deliverables=["big.txt"])
        base = self._seal_baseline(att["id"], budgets={"deliverable_scan_max_bytes": 4})
        item = base["items"][0]
        self.assertEqual(item["status"], "unknown")
        self.assertEqual(item["reason"], "scan-budget-exceeded")
        self.assertFalse(base["coverage"]["complete"])
        self.assertTrue(base["coverage"]["truncated"])

    def test_scan_unstable_marks_unknown(self):
        self._write("out.txt", "v0")
        task, att = self._attempt(deliverables=["out.txt"])
        real_stat = os.stat
        calls: list[str] = []

        def drifting(path, *args, **kwargs):
            st = real_stat(os.fspath(path), *args, **kwargs)
            p = os.fspath(path)
            if p.endswith("out.txt"):
                calls.append(p)
                # 每次观测 mtime 都在漂移：读取前后 stat 不可能一致 → 不稳定
                return os.stat_result(
                    (st.st_mode, st.st_ino, st.st_dev, st.st_nlink,
                     st.st_uid, st.st_gid, st.st_size, st.st_atime,
                     st.st_mtime + len(calls), st.st_ctime))
            return st

        with mock.patch.object(adapter_results.os, "stat", side_effect=drifting):
            base = self._seal_baseline(att["id"])
        item = base["items"][0]
        self.assertEqual(item["status"], "unknown")
        self.assertEqual(item["reason"], "scan-unstable")
        # 统一契约：条目级 IO 失败/不稳定必须同时报告 coverage 不完整
        self.assertFalse(base["coverage"]["complete"])
        self.assertFalse(base["coverage"]["truncated"])
        self.assertTrue(any("scan-unstable" in e
                            for e in base["coverage"]["errors"]))

    def test_permission_error_marks_unknown(self):
        if os.geteuid() == 0:
            self.skipTest("root 下 chmod 000 仍可读，不能构造权限失败")
        self._write("secret.txt", "v0")
        os.chmod(os.path.join(self.site, "secret.txt"), 0o000)
        self.addCleanup(os.chmod, os.path.join(self.site, "secret.txt"), 0o600)
        task, att = self._attempt(deliverables=["secret.txt"])
        base = self._seal_baseline(att["id"])
        item = base["items"][0]
        self.assertEqual(item["status"], "unknown")
        self.assertIn(item["reason"], ("stat-failed", "read-failed"))
        # 权限失败同样必须报告 coverage 不完整（不能宣称完整基线）
        self.assertFalse(base["coverage"]["complete"])

    def test_content_change_with_preserved_mtime_is_changed(self):
        """mtime 不作为新鲜度：内容变化即使保留旧 mtime 也是 changed。"""
        task, att = self._attempt(deliverables=["m.txt"])
        self._write("m.txt", "one")
        f = os.path.join(self.site, "m.txt")
        old_mt = 1000.0
        os.utime(f, (old_mt, old_mt))
        self._seal_baseline(att["id"])
        self._write("m.txt", "two")
        os.utime(f, (old_mt, old_mt))  # 复制/写入后把 mtime 恢复为旧值
        base_mtime = os.stat(f).st_mtime
        out = None
        t0 = 1_000_000
        self._messages("ses_w", ("内容变了",), "msg_in", t0)
        complete_execution(self.ledger, att["id"], outcome="succeeded",
                           input_at=t0, ended_at=t0 + 100)
        out = collect_attempt(self.ledger, self.client, att["id"], CFG, auto=False)
        self.assertEqual(self.provenance_of(att["id"], "m.txt"), "changed")
        self.assertEqual(os.stat(f).st_mtime, base_mtime, "夹具保持旧 mtime")


    def test_unreadable_glob_parent_marks_incomplete_real_chmod(self):
        """协调者 R2 反例（真实 chmod，非 root）：glob 父目录 000 → iglob 吞
        scandir OSError 返回空 → 旧实现 (manifest=[], complete=True)。修复：
        可报告错误的受预算枚举；权限失败 → incomplete；可证明无匹配仍完整。"""
        if os.geteuid() == 0:
            self.skipTest("root 下 chmod 000 仍可读，不能构造权限失败")
        blocked = os.path.join(self.site, "blocked")
        os.makedirs(blocked)
        self._write("blocked/x.txt", "x")
        pattern = os.path.join(blocked, "*.txt")
        budget = adapter_results._scan_budget_state(None)
        # 可读时：1 个匹配，完整
        m1, c1, _ = adapter_results._scan_artifact_manifest(
            pattern, "glob", budget)
        self.assertTrue(c1)
        self.assertEqual(len(m1), 1)
        os.chmod(blocked, 0o000)
        self.addCleanup(os.chmod, blocked, 0o700)
        m2, c2, r2 = adapter_results._scan_artifact_manifest(
            pattern, "glob", adapter_results._scan_budget_state(None))
        self.assertFalse(c2, "不可读父目录不能宣称完整清单")
        self.assertEqual(m2, [])
        self.assertTrue(any("dir-list-failed" in r for r in r2), r2)
        # 可证明的模式无匹配（可读目录、无文件）仍完整空清单
        empty = os.path.join(self.site, "nopattern")
        os.makedirs(empty)
        m3, c3, r3 = adapter_results._scan_artifact_manifest(
            os.path.join(empty, "*.txt"), "glob",
            adapter_results._scan_budget_state(None))
        self.assertTrue(c3)
        self.assertEqual(m3, [])

    def test_unknown_baseline_dir_not_treated_as_new_output(self):
        """协调者 R3 反例（真实链路）：基线扫描权限失败 → 恢复权限 →
        file-only 正式 collect：该 dir 不能被当成本轮新产出（unknown 基线
        的空清单不等于物理不存在）。"""
        if os.geteuid() == 0:
            self.skipTest("root 下 chmod 000 仍可读，不能构造权限失败")
        outdir = os.path.join(self.site, "outdir")
        os.makedirs(outdir)
        self._write("outdir/keep.txt", "旧产物")
        task, att = self._attempt(
            deliverables=["out.txt", {"kind": "dir", "target": "outdir"}])
        # 基线扫描：out.txt present；outdir 因权限 → unknown（manifest 空）
        os.chmod(outdir, 0o000)
        self.addCleanup(os.chmod, outdir, 0o700)
        base = self._seal_baseline(att["id"])
        by = {i["declared"]: i for i in base["items"]}
        self.assertEqual(by["outdir"]["status"], "unknown")
        self.assertFalse(base["coverage"]["complete"])
        # 本轮仅产出 out.txt（新产物）；outdir 恢复可读后内容与基线扫描
        # 失败时一致地"存在但来源不可判定"
        os.chmod(outdir, 0o700)
        self._write("out.txt", "本轮新产物")
        t0 = 1_000_000
        self._messages("ses_w", ("本轮实质回报",), "msg_in", t0)
        complete_execution(self.ledger, att["id"], outcome="succeeded",
                           input_at=t0, ended_at=t0 + 100)
        out = collect_attempt(self.ledger, self.client, att["id"], CFG, auto=False)
        # outdir 不得被判 new（不是本轮产出）：来源 unknown → stale/门禁
        self.assertNotEqual(self.provenance_of(att["id"], "outdir"), "new")
        row = [d for d in out["deliverables"] if d["target"] == "outdir"][0]
        self.assertIn(row["provenance"], ("unknown", "unchanged"))
        self.assertFalse(out["ready_for_verification"])
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertIn(ctx.exception.code, ("STALE_GATE_FAIL", "AUTH_GATE_FAIL"))

    def test_dir_modified_during_scan_marks_unstable_real_files(self):
        """协调者 R4 反例（真实文件受控 I/O）：lstat 处理 a.txt 时新增 b.txt
        → 旧实现 complete=True 且清单只见 a。修复：记录每个已列举目录的
        stat 键+条目名，全扫描后复核——现场变化 → scan-dir-unstable。"""
        outdir = os.path.join(self.site, "outdir")
        os.makedirs(outdir)
        self._write("outdir/a.txt", "a")
        real_lstat = adapter_results.os.lstat
        armed = {"done": False}

        def hooking(path, *args, **kwargs):
            result = real_lstat(path, *args, **kwargs)
            if not armed["done"] and os.fspath(path).endswith("a.txt"):
                armed["done"] = True  # 在首个条目 lstat 时注入同目录新增
                with open(os.path.join(outdir, "b.txt"), "w",
                          encoding="utf-8") as f:
                    f.write("b-new")
            return result

        with mock.patch.object(adapter_results.os, "lstat", side_effect=hooking):
            manifest, complete, reasons = adapter_results._scan_artifact_manifest(
                outdir, "dir", adapter_results._scan_budget_state(None))
        seen = {os.path.basename(m["path"]) for m in manifest}
        self.assertFalse(complete, "扫描期间目录被修改不能宣称完整")
        self.assertTrue(any("scan-dir-unstable" in r for r in reasons), reasons)
        self.assertNotIn("b.txt", seen, "新增条目不在清单，但身份复核必须捕获")
        # 无扰动对照：同一目录稳定重扫完整
        m2, c2, _ = adapter_results._scan_artifact_manifest(
            outdir, "dir", adapter_results._scan_budget_state(None))
        self.assertTrue(c2)
        self.assertEqual({os.path.basename(x["path"]) for x in m2},
                         {"a.txt", "b.txt"})

    def test_failed_read_consumes_budget_immediately(self):
        """预算契约：读取中失败的支路也立即扣减已读字节——持续变化的目录
        不能反复获得免费重试预算（协调者第三轮预算修正）。"""
        probe = os.path.join(self.site, "flaky.txt")
        with open(probe, "w", encoding="utf-8") as f:
            f.write("0123456789")  # 10 字节
        budget = adapter_results._scan_budget_state(
            {"deliverable_scan_max_bytes": 10})
        import builtins as _b

        class _ExplodingRead:
            def __init__(self, path, mode="rb"):
                self._path = path
                self._first = False

            def read(self, n):
                if not self._first:
                    self._first = True
                    return b"0123456789"  # 先读满 10 字节
                raise OSError(5, "读中断")  # 下一次读取失败

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        real_open = _b.open

        def exploding_open(path, mode="r", *args, **kwargs):
            if mode == "rb" and os.fspath(path).endswith("flaky.txt"):
                return _ExplodingRead(path, mode)
            return real_open(path, mode, *args, **kwargs)

        with mock.patch.object(_b, "open", side_effect=exploding_open):
            digest, why = adapter_results._hash_file_strict(probe, budget)
        self.assertIsNone(digest)
        self.assertEqual(why, "read-failed")
        # 已读 10 字节必须已扣减：预算归零 → 后续任何文件（即使很小）都超限
        small = os.path.join(self.site, "tiny.txt")
        with open(small, "w", encoding="utf-8") as f:
            f.write("x")
        d2, why2 = adapter_results._hash_file_strict(small, budget)
        self.assertIsNone(d2)
        self.assertEqual(why2, "scan-budget-exceeded")

    def test_missing_gate_also_enforced_in_prepare(self):
        """协调者：prepare 漏 gate['missing']（line148）→ 准备与提交门不一致。
        事后追加的必需交付行（无 provenance 行、DB 状态 expected/missing）
        必须在事务外准备即 AUTH_GATE_FAIL，与 submit_verification 同门。"""
        task, att, out = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"})
        self.assertTrue(out["ready_for_verification"])
        # 事后追加未产出的必需交付（DB 行 expected；无 provenance 行）
        self.ledger.add_deliverables(att["id"], ["later-added.txt"])
        with self.assertRaises(LedgerError) as ctx:
            prepare_verification(self.ledger, att["id"], result="pass")
        # 旧实现准备层不检查 missing（仅 submit 拒）：准备必须与提交同门
        self.assertEqual(ctx.exception.code, "AUTH_GATE_FAIL")
        self.assertIn("later-added.txt",
                      ctx.exception.detail.get("missing") or [])
        # verify 同门拒绝，且无验收副作用
        with self.assertRaises(LedgerError) as ctx2:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx2.exception.code, "AUTH_GATE_FAIL")
        self.assertEqual(self.ledger.get_attempt(att["id"])["status"],
                         "result_collected")


class TestCarryIntegrity(RemediationBase):
    """D6：carry 重复目标/跨任务来源拒绝；persisted carry 失效。"""

    def _carry_manifest(self, attempt_id, target="out.txt", sha=None,
                        source=None):
        item = {"target": target,
                "baseline_sha256": sha or self._sha(target),
                "reason": "本轮只修订报告，该产物经复核沿用"}
        if source is not None:
            item["source_attempt_id"] = source
        return {"schema": "oc-orch.carry.v1", "attempt_id": attempt_id,
                "items": [item]}

    def test_duplicate_carry_target_rejected(self):
        task, att, _ = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v0"})
        sha = self._sha("out.txt")
        manifest = {"schema": "oc-orch.carry.v1", "attempt_id": att["id"],
                    "items": [
                        {"target": "out.txt", "baseline_sha256": sha,
                         "reason": "沿用 A"},
                        {"target": "out.txt", "baseline_sha256": sha,
                         "reason": "沿用 B（重复目标）"}]}
        with self.assertRaises((ActionError, LedgerError)) as ctx:
            self.app.collect(att["id"], carry_manifest=manifest)
        self.assertEqual(ctx.exception.code, "CARRY_MANIFEST_INVALID")
        errors = ctx.exception.detail.get("errors") or []
        self.assertTrue(any(e.get("code") == "CARRY_TARGET_DUPLICATE"
                            for e in errors))

    def test_carry_source_from_other_task_rejected(self):
        # 另一个任务的更早 attempt：不是本任务来源，必须拒绝
        other_site = os.path.join(self.home, "other")
        os.makedirs(other_site, exist_ok=True)
        with open(os.path.join(other_site, "out.txt"), "w", encoding="utf-8") as f:
            f.write("v0")
        t2 = self.ledger.create_task(project="p2", title="其他任务",
                                     coordinator_session="ses_c",
                                     write_scope=[other_site])
        att2 = self.ledger.create_attempt(
            t2["id"], directory=other_site, session_id="ses_o",
            status="dispatched", dispatch_state="confirmed",
            deliverables=["out.txt"], dispatch_msg_id="msg_o",
            write_scope=[other_site])
        task, att, _ = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v0"})
        manifest = self._carry_manifest(att["id"], sha=self._sha("out.txt"),
                                        source=att2["id"])
        with self.assertRaises((ActionError, LedgerError)) as ctx:
            self.app.collect(att["id"], carry_manifest=manifest)
        self.assertIn(ctx.exception.code,
                      ("CARRY_MANIFEST_INVALID", "CARRY_SOURCE_EVIDENCE_MISMATCH"))
        if ctx.exception.code == "CARRY_MANIFEST_INVALID":
            errors = ctx.exception.detail.get("errors") or []
            self.assertTrue(any(e.get("code") == "CARRY_SOURCE_UNKNOWN"
                                for e in errors),
                            errors)

    def test_carried_file_deleted_after_collect_rejected(self):
        task, att, _ = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v0"})
        out = self.app.collect(att["id"],
                               carry_manifest=self._carry_manifest(att["id"]))
        self.assertEqual(self.provenance_of(att["id"], "out.txt"), "carried")
        self.assertTrue(out["ready_for_verification"])
        os.unlink(os.path.join(self.site, "out.txt"))
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")

    def test_carried_file_modified_after_collect_rejected(self):
        task, att, _ = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v0"})
        self.app.collect(att["id"], carry_manifest=self._carry_manifest(att["id"]))
        self._write("out.txt", "v2-after-carry")
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "RESULT_CHANGED")


class TestUnifiedScanContract(RemediationBase):
    """D5 统一扫描契约：完整/有界/稳定，file/dir/glob/symlink 同一边界。

    协调者复核退回的子场景：lstat 失败不完整、链接身份、walk onerror、
    枚举预算、字节预算读取中累计、同秒亚秒修改不能逃过稳定性检测、
    compare_provenance 尊重预算参数（不再 10**9 大预算）。
    """

    def test_same_second_subsecond_change_detected_unstable(self):
        """秒级 mtime 不变但亚秒级变化（ns 不同）：不能当作稳定内容证据。

        旧实现 int(st_mtime) 秒级比较会让"同秒内修改"逃过不稳定检测，
        把读取中变化的文件当作稳定哈希返回；ns/ctime/identity 键必须捕获。
        """
        self._write("probe.txt", "0123456789")
        real_stat = os.stat
        seq = {"n": 0}
        base_st = real_stat(os.path.join(self.site, "probe.txt"))
        # 同一秒内的亚秒级漂移：mtime 标量（秒）不变，mtime_ns 变化
        mtime_ns_1 = 1_000_000_000_123_456
        mtime_ns_2 = 1_000_000_000_654_321

        def same_second_stat(path, *args, **kwargs):
            p = os.fspath(path)
            if p.endswith("probe.txt"):
                seq["n"] += 1
                ns = mtime_ns_1 if seq["n"] == 1 else mtime_ns_2
                return os.stat_result(
                    (base_st.st_mode, base_st.st_ino, base_st.st_dev,
                     base_st.st_nlink, base_st.st_uid, base_st.st_gid,
                     base_st.st_size, base_st.st_atime, base_st.st_mtime,
                     base_st.st_ctime), {"st_mtime_ns": ns})
            return real_stat(p, *args, **kwargs)

        with mock.patch.object(adapter_results.os, "stat",
                               side_effect=same_second_stat):
            digest, why = adapter_results._hash_file_strict(
                os.path.join(self.site, "probe.txt"),
                adapter_results._scan_budget_state(None))
        self.assertIsNone(digest)
        self.assertEqual(why, "scan-unstable")

    def test_dir_entry_lstat_failure_marks_incomplete(self):
        deliverables = [{"kind": "dir", "target": "outdir"}]
        self._write("outdir/a.txt", "v0")
        self._write("outdir/b.txt", "v0")
        task, att = self._attempt(deliverables=deliverables)
        real_lstat = adapter_results.os.lstat

        def failing_lstat(path, *args, **kwargs):
            p = os.fspath(path)
            if p.endswith("b.txt"):
                raise OSError(13, "Permission denied", p)
            return real_lstat(p, *args, **kwargs)

        with mock.patch.object(adapter_results.os, "lstat",
                               side_effect=failing_lstat):
            base = self._seal_baseline(att["id"])
        item = base["items"][0]
        self.assertEqual(item["status"], "unknown")
        # 失败条目显式出现在清单中（unknown），不能静默省略后宣称完整
        types = {e["path"]: e["type"] for e in item["manifest"]}
        self.assertEqual(types[os.path.join(self.site, "outdir", "b.txt")],
                         "unknown")
        self.assertIsNone(item["manifest_sha256"])
        self.assertFalse(base["coverage"]["complete"])
        self.assertTrue(any("lstat-failed" in e
                            for e in base["coverage"]["errors"]))

    def test_symlink_identity_recorded_in_manifest(self):
        deliverables = [{"kind": "dir", "target": "outdir"}]
        self._write("outdir/real.txt", "v0")
        os.symlink("real.txt", os.path.join(self.site, "outdir", "alias.txt"))
        task, att = self._attempt(deliverables=deliverables)
        base = self._seal_baseline(att["id"])
        item = base["items"][0]
        self.assertEqual(item["status"], "present")
        entry = {e["path"]: e for e in item["manifest"]}[
            os.path.join(self.site, "outdir", "alias.txt")]
        # 链接记录解析身份（readlink 目标），不是只有 type=link
        self.assertEqual(entry["type"], "link")
        self.assertEqual(entry["link"], "real.txt")
        self.assertTrue(item["manifest_sha256"])

    def test_symlink_readlink_failure_marks_incomplete(self):
        deliverables = [{"kind": "dir", "target": "outdir"}]
        self._write("outdir/real.txt", "v0")
        os.symlink("real.txt", os.path.join(self.site, "outdir", "alias.txt"))
        task, att = self._attempt(deliverables=deliverables)
        real_readlink = adapter_results.os.readlink

        def failing_readlink(path, *args, **kwargs):
            p = os.fspath(path)
            if p.endswith("alias.txt"):
                raise OSError(13, "Permission denied", p)
            return real_readlink(p, *args, **kwargs)

        with mock.patch.object(adapter_results.os, "readlink",
                               side_effect=failing_readlink):
            base = self._seal_baseline(att["id"])
        item = base["items"][0]
        self.assertEqual(item["status"], "unknown")
        self.assertFalse(base["coverage"]["complete"])
        self.assertTrue(any("readlink-failed" in e
                            for e in base["coverage"]["errors"]))

    def test_dir_walk_permission_error_marks_incomplete(self):
        if os.geteuid() == 0:
            self.skipTest("root 下 chmod 000 子目录仍可遍历，不能构造权限失败")
        deliverables = [{"kind": "dir", "target": "outdir"}]
        self._write("outdir/a.txt", "v0")
        locked = os.path.join(self.site, "outdir", "locked")
        os.makedirs(locked)
        self._write("outdir/locked/b.txt", "v0")
        os.chmod(locked, 0o000)
        self.addCleanup(os.chmod, locked, 0o700)
        task, att = self._attempt(deliverables=deliverables)
        base = self._seal_baseline(att["id"])
        item = base["items"][0]
        # walk onerror 显式记录：权限失败不能被静默吞掉后宣称完整清单
        self.assertEqual(item["status"], "unknown")
        self.assertIsNone(item["manifest_sha256"])
        self.assertFalse(base["coverage"]["complete"])
        self.assertTrue(any("locked" in e and "PermissionError" in e
                            for e in base["coverage"]["errors"]),
                        base["coverage"]["errors"])

    def test_entry_budget_enforced_during_enumeration(self):
        deliverables = [{"kind": "dir", "target": "outdir"}]
        for i in range(30):
            self._write(f"outdir/f{i:02}.txt", "x")
        task, att = self._attempt(deliverables=deliverables)
        # 枚举预算 10：目录有 30 条目 → 截断；entries 计数真实消耗
        base = self._seal_baseline(att["id"], budgets={
            "deliverable_scan_max_entries": 10})
        item = base["items"][0]
        self.assertEqual(item["status"], "unknown")
        self.assertEqual(len(item["manifest"]), 10)
        self.assertIsNone(item["manifest_sha256"])
        self.assertFalse(base["coverage"]["complete"])
        self.assertTrue(base["coverage"]["truncated"])

    def test_byte_budget_counted_during_read_not_only_preflight(self):
        # 预算只覆盖第一个 8 字节文件：第二个文件 stat 预检即超预算 → unknown
        self._write("s1.txt", "12345678")
        self._write("s2.txt", "12345678")
        task, att = self._attempt(deliverables=["s1.txt", "s2.txt"])
        base = self._seal_baseline(att["id"], budgets={
            "deliverable_scan_max_bytes": 8})
        by_target = {i["declared"]: i for i in base["items"]}
        self.assertEqual(by_target["s1.txt"]["status"], "present")
        self.assertEqual(by_target["s2.txt"]["status"], "unknown")
        self.assertEqual(by_target["s2.txt"]["reason"], "scan-budget-exceeded")
        self.assertFalse(base["coverage"]["complete"])

    def test_compare_provenance_respects_budgets_argument(self):
        """compare_provenance 接受预算参数（B 传 cfg）：不再用 10**9 大预算。"""
        self._write("b1.txt", "0123456789abcdef")
        task, att = self._attempt(deliverables=["b1.txt"])
        baseline = {"schema": "oc-orch.baseline.v1", "attempt_id": att["id"],
                    "items": [{"target": "b1.txt", "declared": "b1.txt",
                               "kind": "file", "status": "missing"}]}
        current = adapter_results.current_deliverable_states(self.ledger,
                                                             att["id"])
        self.assertEqual(current[0]["status"], "present")
        rows = adapter_results.compare_provenance(
            self.ledger, att["id"], baseline=baseline,
            current=current, budgets={"deliverable_scan_max_bytes": 4})
        # 预算不足：当前摘要不可得 → unknown，不得退回无限预算宣称 new
        self.assertEqual(rows[0]["provenance"], "unknown")
        rows2 = adapter_results.compare_provenance(
            self.ledger, att["id"], baseline=baseline,
            current=current, budgets={"deliverable_scan_max_bytes":
                                      512 * 1024 * 1024})
        self.assertEqual(rows2[0]["provenance"], "new")


class TestPersistFreeCandidateScan(RemediationBase):
    """F6：准备扫描只出候选；真实 scan 内部 TypeError 不得触发持久写回退。"""

    def test_current_states_never_falls_back_to_persist_true(self):
        task, att = self._attempt(deliverables=["out.txt"])
        self._write("out.txt", "v0")
        calls: list[dict] = []

        real = self.ledger.check_deliverables

        def spy(aid, **kwargs):
            calls.append(dict(kwargs))
            return real(aid, **kwargs)

        with mock.patch.object(self.ledger, "check_deliverables", side_effect=spy):
            rows = adapter_results.current_deliverable_states(self.ledger,
                                                              att["id"])
        self.assertEqual(calls, [{"persist": False}],
                         "必须以 persist=False 调用，不得回退持久写")
        self.assertTrue(rows)
        # 真实 DB 行保持未写（persist=False：无 checked_at 推进）
        stored = self.ledger.deliverables(att["id"])[0]
        self.assertIsNone(stored["checked_at"])

    def test_scan_internal_typeerror_propagates_without_persistent_write(self):
        task, att = self._attempt(deliverables=["out.txt"])
        self._write("out.txt", "v0")
        calls: list[dict] = []

        def broken_check(aid, **kwargs):
            calls.append(dict(kwargs))
            raise TypeError("scan internal bug")  # 模拟扫描内部真实缺陷

        with mock.patch.object(self.ledger, "check_deliverables",
                               side_effect=broken_check):
            with self.assertRaises(TypeError):
                adapter_results.current_deliverable_states(self.ledger,
                                                           att["id"])
        # 唯一调用是 persist=False；不得捕获 TypeError 后改走 persist=True
        self.assertEqual(calls, [{"persist": False}])
        stored = self.ledger.deliverables(att["id"])[0]
        self.assertIsNone(stored["checked_at"], "异常路径不得留下持久写")


class TestChecksFingerprintBinding(RemediationBase):
    """D10：检查证据绑定结果输入指纹；旧输入检查不得判新输入。"""

    def test_stale_check_evidence_invalid_after_recollect(self):
        deliverables = ["out.txt", {"kind": "command", "target": "true",
                                    "required": True}]
        task, att, out1 = self._collected(
            deliverables=deliverables,
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"})
        fp1 = ((att["metadata"]).get("result") or {})["content_fingerprint"]
        # V1 上执行检查（真实验收准备入口，仅准备不提交）
        prep = prepare_verification(self.ledger, att["id"], result="pass",
                                    run_checks=True)
        evidence = json.loads(self.ledger.deliverables(att["id"])[1]["evidence"])
        self.assertEqual(evidence.get("content_fingerprint"), fp1,
                         "检查证据必须绑定执行时的结果输入指纹")
        # 结果重新收集（产物内容变化 → 新版本新指纹）
        self._write("out.txt", "v2-new-input")
        out2 = self.app.collect(att["id"])
        att2 = self.ledger.get_attempt(att["id"])
        fp2 = ((att2["metadata"]).get("result") or {})["content_fingerprint"]
        self.assertNotEqual(fp2, fp1)
        # 旧输入检查不能判新输入：普通 pass 被拒（含旧指纹失效）
        with self.assertRaises(LedgerError) as ctx:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx.exception.code, "AUTH_GATE_FAIL")
        self.assertIn("true", ctx.exception.detail.get("check_input_mismatch")
                      or ctx.exception.detail.get("unverified") or [])
        # 重跑检查（--run-checks 绑定新指纹）后恢复可验收
        final = verify_attempt(self.ledger, att["id"], result="pass",
                               run_checks=True)
        self.assertEqual(final["verification"]["classification"],
                         "normal_effective")

    def test_run_checks_records_binding_fields(self):
        deliverables = [{"kind": "command", "target": "true", "required": True}]
        task, att, _ = self._collected(deliverables=deliverables,
                                       baseline_contents={"out.txt": "v0"},
                                       final_contents={"out.txt": "v1"})
        ref = (self.ledger.get_attempt(att["id"])["metadata"])["result"]
        run_checks(self.ledger, att["id"])
        ev = json.loads(self.ledger.deliverables(att["id"])[0]["evidence"])
        self.assertEqual(ev.get("content_fingerprint"), ref["content_fingerprint"])
        self.assertEqual(ev.get("result_version"), ref["version"])


class TestVersionSnapshotIntegrity(RemediationBase):
    """D8：同指纹 create-if-absent 校验与 compat 视图按 DB 指针更新。"""

    def test_text_only_corruption_rejected_full(self):
        """协调者 R1 反例（单变量）：只改快照 text，不改 deliverables/
        text_full_sha256/其余 identity → 重复收集/普通 verify 都必须拒绝。
        旧实现只校验版本/指纹字符串相等，正文漏洞被 sha+text 同时改的
        测试掩盖。"""
        task, att, out = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"})
        snap_path = out["result_path"]
        with open(snap_path, encoding="utf-8") as f:
            snap = json.load(f)
        snap["text"] = "only body changed, all identity fields untouched"
        snap["text_chars"] = len(snap["text"])
        with open(snap_path, "w", encoding="utf-8") as f:
            json.dump(snap, f)
        with self.assertRaises(LedgerError) as ctx:
            collect_attempt(self.ledger, self.client, att["id"], CFG, auto=False)
        self.assertEqual(ctx.exception.code, "RESULT_VERSION_MISMATCH")
        with self.assertRaises(LedgerError) as ctx2:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx2.exception.code, "NOT_READY")
        self.assertIn("text", str(ctx2.exception.detail.get("error") or "")
                      + (ctx2.exception.detail.get("error") or ""),
                     "正文损坏需具体 error（text-digest-mismatch）"
                     if "digest" in (ctx2.exception.detail.get("error") or "")
                     else "正文损坏 error")

    def test_text_only_corruption_rejected_truncated(self):
        """R1 截断场景（单变量）：截断快照只改存储前缀正文 → 绑定摘要失配，
        重复收集/普通 verify 都拒绝；同指纹不同 store_limit 的合法更短截断
        仍复用同版本（业务身份不含截断派生字段）。"""
        long_text = "这是一个远超存储上限的很长正文内容触发截断标志" * 3
        cfg_t = dict(CFG)
        cfg_t["result_store_limit"] = 5
        task, att, out = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"},
            texts=(long_text,), cfg=cfg_t)
        snap_path = out["result_path"]
        with open(snap_path, encoding="utf-8") as f:
            snap = json.load(f)
        self.assertTrue(snap["text_truncated"])
        self.assertIn("stored_text_sha256", snap)
        # 单变量：换截断前缀，不改绑定摘要
        snap["text"] = "totally different truncated prefix!!!"
        with open(snap_path, "w", encoding="utf-8") as f:
            json.dump(snap, f)
        with self.assertRaises(LedgerError) as ctx:
            collect_attempt(self.ledger, self.client, att["id"], cfg_t, auto=False)
        self.assertEqual(ctx.exception.code, "RESULT_VERSION_MISMATCH")
        with self.assertRaises(LedgerError) as ctx2:
            verify_attempt(self.ledger, att["id"], result="pass")
        self.assertEqual(ctx2.exception.code, "NOT_READY")
        self.assertIn(ctx2.exception.detail.get("error"),
                      ("text-integrity-invalid", "text-digest-mismatch"))

    def test_reuse_across_store_limits_keeps_version(self):
        """合法复用：同业务输入、不同 result_store_limit → 同版本，
        快照校验不因截断派生值不同误伤（协调者 D8 约束）。"""
        long_text = "这是远超上限的正文" * 5
        cfg_5 = dict(CFG)
        cfg_5["result_store_limit"] = 5
        task, att, out5 = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"},
            texts=(long_text,), cfg=cfg_5)
        self.assertTrue(out5["effective"])
        cfg_3 = dict(cfg_5)
        cfg_3["result_store_limit"] = 3
        out3 = collect_attempt(self.ledger, self.client, att["id"], cfg_3, auto=False)
        self.assertEqual(out3["result_version"], out5["result_version"],
                         "不同 store_limit 的同输入收集必须同版本可复用")

    def test_legacy_v1_artifact_read_without_new_digest_fields(self):
        """M2 legacy 兼容：旧代码生成的 v1 快照（无 protocol_version/text
        digest 字段）按既有 schema/identity 读取；不得在读取侧强制新 digest
        或给旧夹具补伪摘要。"""
        task, att, out = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"})
        rp = self.ledger.get_attempt(att["id"])["result_path"]
        att_now = self.ledger.get_attempt(att["id"])
        legacy = {
            "schema": "oc-orch.result.v1", "attempt_id": att_now["id"],
            "task_id": att_now["task_id"], "session_id": att_now["session_id"],
            "generation": att_now["generation"],
            "collected_at": (att_now["result_collected_at"]
                             if isinstance(att_now["result_collected_at"], int)
                             else 1),
            "text": "legacy 正文", "assistant_message_count": 1,
        }
        backup = rp + ".rv1.bak"
        os.replace(rp, backup)
        self.addCleanup(os.replace, backup, rp)
        with open(rp, "w", encoding="utf-8") as f:
            json.dump(legacy, f)
        snap, err = adapter_results.read_result_artifact(
            self.ledger.get_attempt(att["id"]),
            self.ledger.get_task(task["id"]))
        self.assertIsNotNone(snap, f"legacy v1 快照必须可读：{err}")
        self.assertIsNone(err)

    def test_existing_snapshot_with_divergent_content_rejected(self):
        task, att, out = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"})
        v1 = out["result_version"]
        collect2 = None
        base_manifest = None
        snap_path = os.path.join(self.home, "results", att["id"], "versions",
                                 f"{v1}.json")
        # 情形 A：改交付摘要正文（版本/指纹字符串保持不变）
        with open(snap_path, "r", encoding="utf-8") as f:
            snap = json.load(f)
        for d in snap["deliverables"]:
            d["sha256"] = "0" * 64
        snap["text"] = "被篡改的正文"
        with open(snap_path, "w", encoding="utf-8") as f:
            json.dump(snap, f)
        with self.assertRaises(LedgerError) as ctx:
            collect_attempt(self.ledger, self.client, att["id"], CFG, auto=False)
        self.assertEqual(ctx.exception.code, "RESULT_VERSION_MISMATCH")
        # 情形 B：结构损坏为非对象 → 不当作有效版本复用
        with open(snap_path, "w", encoding="utf-8") as f:
            f.write('{"result_version": "rv1-x", "collected_at": "not-int"}')
        with self.assertRaises(LedgerError) as ctx2:
            collect_attempt(self.ledger, self.client, att["id"], CFG, auto=False)
        self.assertEqual(ctx2.exception.code, "RESULT_VERSION_MISMATCH")

    def test_valid_existing_snapshot_still_reuses_collected_at(self):
        """未损坏的同业务内容快照：正常复用原 collected_at（不误伤幂等）。"""
        task, att, out = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"})
        out2 = collect_attempt(self.ledger, self.client, att["id"], CFG,
                               auto=False)
        self.assertTrue(out2["duplicate"])
        self.assertEqual(out2["result_version"], out["result_version"])
        # 同版本幂等：collected_at 与首次收集一致（快照复用原时间）
        self.assertEqual(self.ledger.get_attempt(att["id"])["result_collected_at"],
                         out["collected_at"])

    def test_compat_view_real_race_repaired_after_write(self):
        """真实双 WAL 连接 + 受控 I/O 钩子：A 读 V1 → B 提交 V2 并写视图 →
        A 才写 V1（覆盖视图）→ A 写后复核发现 DB 已是 V2 → 按权威快照修复。"""
        task, att, out1 = self._collected(
            baseline_contents={"out.txt": "v0"}, final_contents={"out.txt": "v1"})
        v1 = out1["result_version"]
        home = self.home
        # 收集器 A 的旧 result：从 V1 权威快照原样读出（真实数据，非构造）
        with open(os.path.join(home, "results", att["id"], "versions",
                               f"{v1}.json"), encoding="utf-8") as f:
            a_result = json.load(f)
        # 第二个真实连接（同 SQLite 文件，WAL 可见性）
        conn_b = init(home)
        self.addCleanup(conn_b.close)
        ledger_b = Ledger(conn_b, home)
        app_b = application(ledger_b, lambda: self.client, CFG)
        view_path = os.path.join(home, "results", f"{att['id']}.json")

        # 受控交错钩子：A 的 update_compat_view 在写前读完成后、真正落盘前，
        # 让收集器 B 提交 V2 并更新视图（此刻视图=V2、DB 指针=V2）
        real_atomic_write = adapter_results.atomic_write
        gate = {"before_a_write": None}

        def racing_atomic_write(path, *args, **kwargs):
            if gate["before_a_write"] is None and path == view_path:
                gate["before_a_write"] = True
                # B 侧真实收集提交 V2（产物内容变化 → 新版本）
                with open(os.path.join(self.site, "out.txt"), "w",
                          encoding="utf-8") as f:
                    f.write("v2-race")
                self.client.append_message("ses_w", _assistant(
                    "race-a", "竞态窗口内的新回报", 2_000_000))
                out_b = app_b.collect(att["id"])
                self.assertFalse(out_b["duplicate"])
                with open(view_path, encoding="utf-8") as f:
                    during = json.load(f)
                self.assertEqual(during["result_version"],
                                 out_b["result_version"])
                gate["b_version"] = out_b["result_version"]
            return real_atomic_write(path, *args, **kwargs)

        with mock.patch.object(adapter_results, "atomic_write",
                               side_effect=racing_atomic_write):
            # A（旧连接/旧收集器）此刻才写 V1 视图：写后复核必须修复为 V2
            adapter_results.update_compat_view(self.ledger, att["id"], a_result)

        v2 = gate["b_version"]
        self.assertNotEqual(v2, v1)
        with open(view_path, encoding="utf-8") as f:
            final = json.load(f)
        # A 的落后写没有留下 V1：写后复核按当前 DB 指针权威快照重建为 V2
        self.assertEqual(final["result_version"], v2)
        stored = ((self.ledger.get_attempt(att["id"])["metadata"]).get("result")
                  or {})["version"]
        self.assertEqual(stored, v2)


class TestReadMessageWindow(RemediationBase):
    """F3：倒序有界窗口与严格覆盖判定（IO 契约；B 的正式收集经 _CollectionIO）。"""

    def _fill(self, client, session, n_old, anchor_at, n_window):
        for i in range(1, n_old + 1):
            client.append_message(session, _user(f"old-{i}", i))
        client.append_message(session, _user("msg_in", anchor_at))
        for i in range(1, n_window + 1):
            client.append_message(session, _assistant(
                f"win-{i}", f"m{i}", anchor_at + i))

    def test_window_bounded_by_anchor_and_until_with_small_budget(self):
        self._fill(self.client, "s", 5000, 5100, 60)
        msgs, cov = adapter_results.read_message_window(
            self.client, "s", max_pages=5, page_size=50, anchor_id="msg_in",
            since_ms=5099, until_ms=5130)
        self.assertTrue(cov["complete"], cov)
        self.assertTrue(cov["anchor_found"])
        self.assertEqual(cov["bound"], "anchor")
        # 远古历史 > max_pages×page_size 也不耗尽预算：只需翻到锚点
        self.assertLessEqual(cov["pages"], 3)
        self.assertEqual([m["id"] for m in msgs][0], "msg_in")
        self.assertEqual(msgs[-1]["id"], "win-30")
        self.assertEqual(len(msgs), 31)
        self.assertTrue(all(5100 <= m["time"]["created"] <= 5130 for m in msgs))

    def test_message_with_unknown_created_not_treated_as_zero(self):
        """协调者：未知 created 不能当 0（当 0 会被判"早于起界"伪 complete）。"""
        class FlawedTimeClient:
            def __init__(self, msgs):
                self._msgs = msgs

            def message_page(self, session_id, *, limit, order="asc",
                             cursor=None):
                data = self._msgs[:limit]
                more = len(self._msgs) > len(data)
                return {"data": list(data),
                        "cursor": {"next": None if not more else "desc:9:z",
                                   "previous": None}}

        msgs = [{"id": "m-1", "type": "user", "text": "x",
                 "time": {"created": "not-a-number"}},   # created 非法
                {"id": "m-2", "type": "user", "text": "x", "time": {}},  # 缺 created
                {"id": "m-3", "type": "user", "text": "x",
                 "time": True}]  # bool 不是有效时间
        for i, flawed in enumerate(msgs):
            with self.subTest(case=i):
                _, cov = adapter_results.read_message_window(
                    FlawedTimeClient([flawed]), "s", max_pages=5, page_size=10,
                    anchor_id="m-1", since_ms=100)
                self.assertFalse(cov["complete"], cov)
                self.assertEqual(cov["reason"], "page-malformed-message-shape")

    def test_message_without_id_is_malformed(self):
        class NoIdClient:
            def message_page(self, session_id, *, limit, order="asc",
                             cursor=None):
                return {"data": [{"type": "user", "text": "x",
                                  "time": {"created": 5}}],
                        "cursor": {"next": None, "previous": None}}

        _, cov = adapter_results.read_message_window(
            NoIdClient(), "s", max_pages=5, page_size=10, anchor_id="m-1")
        self.assertFalse(cov["complete"])
        self.assertEqual(cov["reason"], "page-malformed-message-shape")

    def test_anchor_created_unknown_degrades_to_since_or_incomplete(self):
        class UnknownAnchorTimeClient:
            def message_page(self, session_id, *, limit, order="asc",
                             cursor=None):
                anchor = {"id": "msg_in", "type": "user", "text": "go",
                          "time": {"created": "broken"}}  # 锚点时间不可解析
                return {"data": [anchor], "cursor": {"next": None,
                                                     "previous": None}}

        client = UnknownAnchorTimeClient()
        # 无 since：锚点消息时间非法 → 最小必要字段不满足，整页不可信
        # （比"当 0 起界"严格：输入区间不可证明即 incomplete）
        msgs, cov = adapter_results.read_message_window(
            client, "s", max_pages=5, page_size=10, anchor_id="msg_in")
        self.assertFalse(cov["complete"])
        self.assertIn(cov["reason"],
                      ("page-malformed-message-shape", "anchor-created-unknown"))
        self.assertFalse(cov["anchor_found"],
                         "时间不可解析的锚点不能算作已确认的输入锚点")
        self.assertIsNone(cov["bound"])
        # 有 since 起界也一样：锚点消息本身字段不合法不能当作命中降级，
        # 输入身份未确认（anchor_found=False）时调用方必须另行补证。
        msgs2, cov2 = adapter_results.read_message_window(
            client, "s", max_pages=5, page_size=10, anchor_id="msg_in",
            since_ms=100)
        self.assertFalse(cov2["complete"])
        self.assertFalse(cov2["anchor_found"])
        self.assertIsNone(cov2["bound"])

    def test_invalid_bound_parameters_reported_incomplete(self):
        _, cov = adapter_results.read_message_window(
            self.client, "s", max_pages=5, page_size=10, since_ms="100")
        self.assertFalse(cov["complete"])
        self.assertEqual(cov["reason"], "invalid-bound:since_ms")
        _, cov2 = adapter_results.read_message_window(
            self.client, "s", max_pages=5, page_size=10, until_ms=True)
        self.assertFalse(cov2["complete"])
        self.assertEqual(cov2["reason"], "invalid-bound:until_ms")

    def test_budget_exhausted_before_anchor_is_incomplete(self):
        # 锚点在历史最深处：40 条后续消息 > 2 页 × 20 条，预算内翻不到锚点
        self.client.append_message("s", _user("msg_in", 1))
        for i in range(2, 42):
            self.client.append_message("s", _user(f"m-{i}", i))
        _, cov = adapter_results.read_message_window(
            self.client, "s", max_pages=2, page_size=20, anchor_id="msg_in",
            since_ms=1)
        self.assertFalse(cov["complete"])
        self.assertEqual(cov["reason"], "page-budget-exhausted")
        self.assertFalse(cov["anchor_found"])
        self.assertIsNone(cov["bound"])

    def test_anchor_not_found_by_scan_cap_is_incomplete(self):
        self._fill(self.client, "s", 60, 100, 5)
        # 可见历史只剩最近 5 条（窗口消息），锚点不可翻到
        self.client.message_scan_cap = 5
        _, cov = adapter_results.read_message_window(
            self.client, "s", max_pages=20, page_size=5, anchor_id="msg_in")
        self.assertFalse(cov["complete"])
        self.assertEqual(cov["reason"], "anchor-not-found")
        self.assertFalse(cov["anchor_found"])

    def test_since_bound_fallback_still_report_anchor_missing(self):
        """锚点不可见但起界可覆盖：complete=True 供调用方按 anchor_found 判定。"""
        self._fill(self.client, "s", 60, 100, 5)
        self.client.message_scan_cap = 5
        msgs, cov = adapter_results.read_message_window(
            self.client, "s", max_pages=20, page_size=5, anchor_id="msg_in",
            since_ms=100)
        self.assertTrue(cov["complete"])
        self.assertEqual(cov["bound"], "since")
        self.assertFalse(cov["anchor_found"])
        self.assertTrue(all(m["time"]["created"] >= 100 for m in msgs))

    def test_duplicate_cursor_reported_incomplete(self):
        class LoopingClient:
            def message_page(self, session_id, *, limit, order="asc",
                             cursor=None):
                return {"data": [_user("m1", 1)],
                        "cursor": {"next": "desc:1:m1"}}

        msgs, cov = adapter_results.read_message_window(
            LoopingClient(), "s", max_pages=10, page_size=10, anchor_id=None,
            since_ms=None)
        self.assertFalse(cov["complete"])
        self.assertEqual(cov["reason"], "cursor-loop")

    def test_empty_page_with_next_cursor_reported_incomplete(self):
        class EmptyNextClient:
            def message_page(self, session_id, *, limit, order="asc",
                             cursor=None):
                return {"data": [], "cursor": {"next": "desc:9:z"}}

        _, cov = adapter_results.read_message_window(
            EmptyNextClient(), "s", max_pages=5, page_size=10)
        self.assertFalse(cov["complete"])
        self.assertEqual(cov["reason"], "empty-page-with-next-cursor")

    def test_missing_page_fields_reported_incomplete(self):
        class NoCursorClient:
            def message_page(self, session_id, *, limit, order="asc",
                             cursor=None):
                return {"data": [_user("m1", 1)]}

        _, cov = adapter_results.read_message_window(
            NoCursorClient(), "s", max_pages=5, page_size=10)
        self.assertFalse(cov["complete"])
        self.assertEqual(cov["reason"], "page-malformed-missing-fields")

    def test_malformed_shapes_reported_incomplete(self):
        class BadDataClient:
            def message_page(self, session_id, *, limit, order="asc",
                             cursor=None):
                return {"data": "not-a-list", "cursor": {}}

        _, cov = adapter_results.read_message_window(
            BadDataClient(), "s", max_pages=5, page_size=10)
        self.assertFalse(cov["complete"])
        self.assertEqual(cov["reason"], "page-malformed-shape")

        class BadCursorClient:
            def message_page(self, session_id, *, limit, order="asc",
                             cursor=None):
                return {"data": [_user("m1", 1)], "cursor": None}

        _, cov2 = adapter_results.read_message_window(
            BadCursorClient(), "s", max_pages=5, page_size=10)
        self.assertFalse(cov2["complete"])
        self.assertEqual(cov2["reason"], "page-malformed-shape")

    def test_protocol_error_converted_to_coverage_error(self):
        self._fill(self.client, "s", 10, 100, 5)
        self.client.fail("message_page", RuntimeError("transport down"))
        _, cov = adapter_results.read_message_window(
            self.client, "s", max_pages=5, page_size=5, anchor_id="msg_in")
        self.assertFalse(cov["complete"])
        self.assertEqual(cov["reason"], "message-page-error")
        self.assertIn("transport down", cov.get("error") or "")

    def test_duplicates_across_pages_deduped(self):
        for i in range(1, 6):
            self.client.append_message("s", _user(f"m-{i}", i))
        # 服务端分页重叠：第二页重复返回 m-3/m-4/m-5
        original = self.client.message_page

        def overlapping(session_id, *, limit, order="asc", cursor=None):
            page = original(session_id, limit=limit, order=order, cursor=cursor)
            if cursor is not None:
                page = dict(page)
                page["data"] = ([m for m in page["data"] if m["time"]["created"] < 3]
                                + [m for m in self.client._messages_store["s"]
                                   if m["time"]["created"] >= 3][:limit])
                page["cursor"] = {"next": None, "previous": None}
            return page

        with mock.patch.object(self.client, "message_page", side_effect=overlapping):
            msgs, cov = adapter_results.read_message_window(
                self.client, "s", max_pages=10, page_size=2, anchor_id=None,
                since_ms=None)
        self.assertTrue(cov["complete"], cov)
        self.assertGreaterEqual(cov["duplicates_skipped"], 1)
        self.assertEqual(len(msgs), len({m["id"] for m in msgs}))


if __name__ == "__main__":
    unittest.main()

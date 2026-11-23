#!/usr/bin/env python3
"""父本轮新增（派单 authority / 共享业务健康）的独立反例审查。

只做只读审查与新增红测：不修改任何 src/其他 tests/runner/docs。全部使用
临时 SQLite（/tmp/opencode）+ FakeClock/FakeClient，零真实服务/生产/邮箱。

目标不变量（与父 12+12 区分，不复制其用例）：

- 派单 authority：认领事务前 WAL 交接必须被 expected-CAS 挡住（不产生尝试）；
  并发两连接派单不得通过事务外 current 读取绕过 single-outstanding，留下
  非当前、已发送 prompt 的孤儿尝试（写权/双执行）；authority 记录移除/损坏
  一律拒绝重发；损坏 lease 行（expires_at 非 int）应干净拒绝而非 TypeError。
- 共享 health：只读快照不推进 runtime 公平游标，且夹在 runtime 两轮之间也不
  重置/污染游标；责任页在 200 上限截断时必须 coverage 不完整、不伪造事件；
  分页恰好 limit 行时完整度为 False 并给出 next_after_id（不冒称完成）。

预期存在实缺口的两类红测（保留红、不修实现，仅供父归因）：
  1) 并发两连接派单（WAL current 改变）可产生 2 条已确认且已 prompt 的尝试；
  2) 损坏 lease 行（expires_at 字符串）经 check_dispatch_authority 抛
     TypeError（未分类），而非稳定 LedgerError 干净拒绝。
"""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from oc_orch.composition import (
    application,
    init,
    make_continuity_status,
    make_responsibilities,
)
from oc_orch.config import DEFAULTS
from oc_orch.domain.errors import ActionError, LedgerError
from oc_orch.ledger import Ledger
from oc_orch.supervisor import Supervisor
from tests.fakes import FakeClient, FakeClock

TMP = "/tmp/opencode"


def _prepared(ledger, task, *, home, session_id="ses_worker",
              directory=None, msg_id="msg_original"):
    """构造 dispatch_state=prepared、已冻结 dispatch_authority 的尝试。"""
    site = directory or (Path(home) / "worker-site")
    return ledger.create_attempt(
        task["id"], directory=str(site), session_id=session_id,
        dispatch_msg_id=msg_id, metadata={
            "coordinator": ledger.get_task(task["id"])["coordinator_session"],
            "dispatch": {
                "session_id_planned": session_id, "prompt": "work",
                "create": {"directory": str(site)}}})


class DispatchAuthorityCounterTests(unittest.TestCase):
    """WAL 交接/current 改变/authority 损坏/损坏 lease 行的红绿对照。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="parent-safety-", dir=TMP)
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.clock = FakeClock()
        self.client = FakeClient()
        self.sup = Supervisor(self.home, cfg=dict(DEFAULTS),
                              client=self.client, clock=self.clock)
        self.addCleanup(self.sup.conn.close)
        self.ledger = self.sup.ledger
        self.task = self.ledger.create_task(project="p", title="work",
                                            coordinator_session="ses_old")
        self.lease = self.ledger.claim_coordinator(
            project="p", session_id="ses_old", actor="test", ttl=3600)
        self.conn2 = init(self.home)
        self.addCleanup(self.conn2.close)
        self.other = Ledger(self.conn2, self.home, clock=self.clock)

    def app(self):
        return application(self.ledger, lambda: self.client, dict(DEFAULTS),
                           clock=self.clock)

    def dispatch(self, *, directory, coordinator="ses_old", **kw):
        return self.app().dispatch(self.task["id"], directory=directory,
                                   coordinator=coordinator, **kw)

    def handoff(self, holder="ses_new"):
        return self.other.claim_coordinator(
            project="p", session_id=holder, actor="test", takeover=True,
            old_confirmed_stopped=True, ttl=3600)

    # ---- 1) 认领事务前 WAL 交接：不得产生尝试/网络 -------------------
    def test_wal_takeover_between_authority_snapshot_and_claim_blocks_before_create(self):
        original = self.ledger.check_dispatch_authority

        def snap_then_handoff(project, session_id=None, *, expected=None):
            out = original(project, session_id, expected=expected)
            if expected is None:  # 事务外首次快照后、认领事务之前
                self.handoff()
            return out

        with patch.object(self.ledger, "check_dispatch_authority",
                          side_effect=snap_then_handoff):
            with self.assertRaises((LedgerError, ActionError)):
                self.dispatch(directory=str(Path(self.home) / "site-a"),
                              prompt="work-a")
        self.assertEqual(self.ledger.list_attempts(), [], "交接后不得创建尝试")
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.client.prompt_calls, [])
        self.assertEqual(
            self.ledger.get_task(self.task["id"])["coordinator_session"],
            "ses_new")

    # ---- 2) 并发两连接派单：single-outstanding 不得被事务外读取绕过 -----
    def test_concurrent_two_connection_dispatch_must_not_create_two_live_attempts(self):
        site_a = Path(self.home) / "site-a"
        site_b = Path(self.home) / "site-b"
        site_a.mkdir()
        site_b.mkdir()
        app_b = application(self.other, lambda: self.client, dict(DEFAULTS),
                            clock=self.clock)
        real = self.ledger.create_attempt

        def create_then_other_dispatch(*args, **kwargs):
            # 本连接 outer current 已读为空；此时另一连接完成一次完整派单
            app_b.dispatch(self.task["id"], directory=str(site_b),
                           coordinator="ses_old", prompt="work-b")
            return real(*args, **kwargs)

        with patch.object(self.ledger, "create_attempt",
                          side_effect=create_then_other_dispatch):
            with self.assertRaises(LedgerError) as rejected:
                self.dispatch(directory=str(site_a), prompt="work-a")
        self.assertEqual(rejected.exception.code, "NOT_CURRENT")

        atts = self.ledger.list_attempts()
        confirmed = [a for a in atts if a["dispatch_state"] == "confirmed"]
        # 期望：两个并发派单只允许一个成为在途执行者（single-outstanding）。
        # 现状：另一个连接的尝试被覆盖为 non-current，但已确认且已 prompt，
        # 遗留非当前、已发送、持写权的孤儿尝试（双执行/写权泄漏）。
        self.assertEqual(len(confirmed), 1,
                         f"并发派单产生 {len(confirmed)} 条已确认尝试："
                         f"{[a['id'] for a in confirmed]}，单执行者不变量被绕过")
        self.assertEqual(len(self.client.prompt_calls), 1,
                         "并发派单不得发送两次 prompt（双执行）")
        current = self.ledger.get_task(self.task["id"])["current_attempt_id"]
        self.assertIn(current, [a["id"] for a in atts])
        for a in atts:
            if a["id"] != current:
                self.assertTrue(a.get("site_released_at") or a["dispatch_state"] != "confirmed",
                                f"非当前尝试 {a['id']} 不得以 confirmed 状态持有写权")

    # ---- 3) authority 损坏/移除：一律拒绝重发，不产生网络 --------------
    def test_removed_authority_with_lease_present_blocks_reprompt(self):
        att = _prepared(self.ledger, self.task, home=self.home)
        meta = dict(att["metadata"])
        meta.pop("dispatch_authority", None)
        self.ledger.conn.execute("UPDATE attempts SET metadata=? WHERE id=?", (json.dumps(meta), att["id"]))
        out = self.sup.reconcile_dispatch(self.ledger.get_attempt(att["id"]))
        self.assertIsNone(out)
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.client.prompt_calls, [])
        after = self.ledger.get_attempt(att["id"])
        self.assertEqual(after["dispatch_msg_id"], "msg_original")

    def test_corrupted_authority_schema_blocks_reprompt(self):
        att = _prepared(self.ledger, self.task, home=self.home)
        meta = dict(att["metadata"])
        meta["dispatch_authority"] = {"schema": 2, "session_id": "ses_old"}
        self.ledger.conn.execute("UPDATE attempts SET metadata=? WHERE id=?", (json.dumps(meta), att["id"]))
        with self.assertRaises((LedgerError, ActionError)):
            self.app().dispatch(self.task["id"],
                                directory=str(Path(self.home) / "site-x"),
                                coordinator="ses_old", prompt="x")
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.client.prompt_calls, [])

    # ---- 4) 历史无 lease 冻结快照之后出现 lease：不得补造授权重发 ------
    def test_no_lease_frozen_authority_then_same_holder_claim_blocks_resend(self):
        self.ledger.release_coordinator("p", actor="test")
        att = _prepared(self.ledger, self.task, home=self.home)
        # 同 holder 重新 claim：epoch/lease_event 从无到有，旧快照不得复用
        self.other.claim_coordinator(project="p", session_id="ses_old",
                                     actor="test", ttl=3600)
        out = self.sup.reconcile_dispatch(self.ledger.get_attempt(att["id"]))
        self.assertIsNone(out)
        self.assertEqual(self.client.prompt_calls, [])

    # ---- 5) 损坏 lease 行（expires_at 非 int）：应稳定 LedgerError 拒绝 --
    def test_corrupt_lease_expires_at_string_is_clean_guard_error(self):
        self.conn2.execute(
            "UPDATE coordinator_leases SET expires_at='far-future' WHERE project='p'")
        self.conn2.commit()
        # 期望：损坏租约行按“损坏一律拒绝”抛稳定 LedgerError（不产生网络）。
        # 现状：continuity_guard 未做类型校验，`str <= int` 抛 TypeError（未分类）。
        with self.assertRaises(LedgerError):
            self.ledger.check_dispatch_authority("p", "ses_old")
        self.assertEqual(self.client.create_calls, [])
        self.assertEqual(self.client.prompt_calls, [])


class ContinuityHealthCounterTests(unittest.TestCase):
    """共享 health：只读快照/公平游标隔离、责任截断不伪造、分页边界。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="parent-health-", dir=TMP)
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.clock = FakeClock()
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        self.lease = self.ledger.claim_coordinator(
            project="p", session_id="ses_coord", actor="test", ttl=3600)
        self.service = make_continuity_status(self.ledger, self.clock)

    def task(self, task_id=None, project="p"):
        return self.ledger.create_task(project=project, title="t",
                                       task_id=task_id,
                                       coordinator_session="ses_coord")

    # ---- A) 只读快照夹在 runtime 两轮之间：不重置/不污染公平游标 -------
    def test_readonly_pagination_between_runtime_cycles_keeps_fair_cursor(self):
        self.task("T-1")
        self.task("T-2")
        self.task("T-3")
        # A) runtime 第一轮：advance_cursor=True
        first = self.service.snapshot(limit=2, advance_cursor=True)
        self.assertEqual(len(first["tasks"]), 2)
        runtime_cursor = self.ledger.get_meta("sweep-cursor:business-health-tasks")
        self.assertTrue(runtime_cursor)
        seen_first = {t["task_id"] for t in first["tasks"]}
        # B) 只读分页（CLI 语义 advance_cursor=False + after_id）：不得写游标
        page = self.service.snapshot(
            limit=1, project="p", after_id=first["coverage"]["next_after_id"])
        self.assertEqual(len(page["tasks"]), 1)
        self.assertEqual(
            self.ledger.get_meta("sweep-cursor:business-health-tasks"),
            runtime_cursor, "只读分页不得覆盖/重置 runtime 公平游标")
        # C) runtime 第二轮：从持久游标继续（两轮 union 公平覆盖全部 3 项）
        second = self.service.snapshot(limit=2, advance_cursor=True)
        seen_second = {t["task_id"] for t in second["tasks"]}
        self.assertNotEqual(seen_second, seen_first)
        self.assertEqual(len(seen_first | seen_second), 3)
        self.assertNotIn(
            "business-health-tasks",
            json.dumps(repr(page)), "分页结果不得泄漏游标内部")

    # ---- B) 责任页 200 上限截断：coverage 不完整、不伪造可行动事件 ------
    def test_responsibility_cap_truncation_is_not_complete_and_no_fabricated_event(self):
        t1 = self.task("T-1")
        good = self.task("T-2")
        store = make_responsibilities(self.ledger)
        for i in range(200):
            store.open_responsibility(
                "coord", "p", t1["id"], attempt_id=None, generation=None,
                result_version=None, target_session="ses_coord",
                state="waiting_input", wait_kind="input_execution",
                wait_condition=f"row-{i}", next_action="wait",
                next_check_at=self.clock.now_ms(),
                deadline=self.clock.now_ms() + 3_600_000,
                condition_key=f"cap-key-{i}", max_budget=3)
        out = self.service.snapshot(limit=50)
        cap_item = next(t for t in out["tasks"] if t["task_id"] == "T-1")
        self.assertFalse(cap_item["coverage_ok"])
        self.assertFalse(cap_item["responsibility_coverage_complete"])
        self.assertFalse(any(e["task_id"] == "T-1" for e in out["events"]),
                         "责任截断读不到全量 ≠ 无责任：不得伪造进度事件")
        self.assertTrue(any(e["task_id"] == "T-2" for e in out["events"]),
                        "其他正常任务的事件不受截断任务影响")
        self.assertEqual(len(cap_item["responsibilities"]), 200)

    # ---- C) 分页恰好 limit 行：完整度必须为 False 且给出 next_after -----
    def test_exactly_limit_page_reports_partial_scope_with_next_after(self):
        self.task("T-1")
        self.task("T-2")
        first = self.service.snapshot(project="p", limit=2)
        self.assertFalse(first["coverage"]["complete"],
                         "恰为 limit 行不得冒称完整覆盖")
        self.assertIsNotNone(first["coverage"]["next_after_id"])
        second = self.service.snapshot(project="p", limit=2,
                                       after_id=first["coverage"]["next_after_id"])
        self.assertEqual(second["tasks"], [])
        self.assertTrue(second["coverage"]["complete"])
        self.assertIsNone(self.ledger.get_meta("sweep-cursor:business-health-tasks"),
                          "只读分页不得写持久游标")


class ReleaseIdentityShapeTests(unittest.TestCase):
    """release manifest 坏形状路径：一律 unverified、不崩溃、不泄漏。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="parent-release-", dir=TMP)
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)

    def identity(self, manifest, *, write_source=True):
        from oc_orch.adapters.release_identity import runtime_identity
        root = Path(self.home) / "release"
        root.mkdir(exist_ok=True)
        if write_source:
            (root / "source.py").write_text("source")
        (root / "RELEASE.json").write_text(json.dumps(manifest))
        return runtime_identity(self.conn, root=root)

    def _good_files(self):
        return {"source.py": hashlib.sha256(b"source").hexdigest()}

    def test_bad_shape_manifest_never_verifies(self):
        files = self._good_files()
        identity = hashlib.sha256(
            json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        cases = {
            "not-object": ["source.py"],
            "files-list": {"runtime_files": ["source.py"],
                           "runtime_manifest_sha256": identity,
                           "release_id": "R1"},
            "files-empty": {"runtime_files": {},
                            "runtime_manifest_sha256": identity,
                            "release_id": "R1"},
            "missing-sha": {"runtime_files": files, "release_id": "R1"},
            "bad-lease-id": {"runtime_files": files,
                             "runtime_manifest_sha256": identity,
                             "release_id": 42},
            "traversal": {"runtime_files": {"../source.py": files["source.py"]},
                          "runtime_manifest_sha256": identity,
                          "release_id": "R1"},
            "absolute": {"runtime_files": {"/tmp/source.py": files["source.py"]},
                         "runtime_manifest_sha256": identity,
                         "release_id": "R1"},
        }
        for name, manifest in cases.items():
            with self.subTest(shape=name):
                out = self.identity(manifest)
                self.assertFalse(out["manifest_verified"], name)
                self.assertIsNone(out["release_id"], name)

    def test_missing_file_entry_unverified_and_no_crash(self):
        files = {"source.py": hashlib.sha256(b"source").hexdigest(),
                 "missing.py": hashlib.sha256(b"x").hexdigest()}
        manifest = {"release_id": "R1", "runtime_files": files,
                    "runtime_manifest_sha256": "x"}
        out = self.identity(manifest)
        self.assertFalse(out["manifest_verified"])
        self.assertIsNone(out["release_id"])


if __name__ == "__main__":
    unittest.main()

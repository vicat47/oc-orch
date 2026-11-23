#!/usr/bin/env python3
"""显式对话控制 CLI 契约（coordinator pause/resume/hold/control/turn-*）。

被测对象只到 entrypoints：参数边界 / JSON / human 输出与旧 block/unlock 兼容；
业务校验在 application.conversation_control，持久化与 CAS 在 worker A 的
CoordinatorControlService（父 composition 装配）。测试用离线替身注入
`_open_coordinator_control`，绝不触网、不读生产台账。native 对话 hook 未接通，
故只测显式本地 operator 入口（fail-closed：metadata 自称 user 永远拿不到控制权）。

真实 factory 会经 `_open_coordinator_control` 惰性装配 make_coordinator_control，
worker A 模块由父提供；本测试不依赖它存在。
"""

from __future__ import annotations

from contextlib import contextmanager, redirect_stderr, redirect_stdout
from io import StringIO
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from oc_orch.entrypoints import cli  # noqa: E402
from oc_orch.adapters.clock import SystemClock  # noqa: E402
from oc_orch.adapters.sqlite_db import init  # noqa: E402
from oc_orch.adapters.sqlite_ledger import Ledger  # noqa: E402
from oc_orch.adapters.target_gate import TargetGate  # noqa: E402


class _FakeControl:
    """CoordinatorControlPort 离线替身：记录调用并返回可布置状态。"""

    def __init__(self):
        self.calls = []
        self.projects = {
            "/p": {"project": "/p", "holder_session": "ses_c", "epoch": 2,
                   "revision": 3, "paused": True,
                   "pause": {"operation_id": "op-orig", "reason": "user-stop",
                             "requested_by": "operator-cli",
                             "requested_at": 9_000_000_000}},
        }
        self.sessions = {
            "ses_c": {"session_id": "ses_c", "projects": ["/p"], "hold": None,
                      "turn": None},
        }
        self.mutation_result = {"executed": True, "changed": True,
                                "applied": True, "revision": 3, "epoch": 2,
                                "snapshot": {"status": "paused"}}

    def _rec(self, method, **kw):
        self.calls.append((method, kw))

    def get_project(self, project):
        self._rec("get_project", project=project)
        return self.projects.get(project)

    def get_session(self, session_id):
        self._rec("get_session", session_id=session_id)
        return self.sessions.get(session_id)

    def pause(self, project, **kw):
        self._rec("pause", project=project, **kw)
        return dict(self.mutation_result)

    def resume(self, project, **kw):
        self._rec("resume", project=project, **kw)
        return dict(self.mutation_result)

    def hold_session(self, session_id, **kw):
        self._rec("hold_session", session_id=session_id, **kw)
        return dict(self.mutation_result)

    def begin_user_turn(self, session_id, **kw):
        self._rec("begin_user_turn", session_id=session_id, **kw)
        return dict(self.mutation_result)

    def finish_user_turn(self, session_id, **kw):
        self._rec("finish_user_turn", session_id=session_id, **kw)
        return dict(self.mutation_result)

    def permits(self, project, session_id):
        self._rec("permits", project=project, session_id=session_id)
        return {"permits": False, "blocking": ["paused"]}


class CoordinatorControlCliTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="coord-ctrl-cli-", dir="/tmp/opencode")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        self.fake = _FakeControl()
        self.opener = self._opener(self.fake)

    @staticmethod
    def _opener(fake):
        @contextmanager
        def opened(home):
            yield fake
        return opened

    def _parse(self, argv):
        return cli.build_parser().parse_args(["--home", self.tmp] + argv)

    def _exec(self, argv, as_json=False):
        raw = (["--json"] if as_json else []) + ["--home", self.tmp]
        raw += argv
        args = cli.build_parser().parse_args(raw)
        out_buf, err_buf = StringIO(), StringIO()
        with patch.object(cli, "_open_coordinator_control", self.opener), \
                redirect_stdout(out_buf), redirect_stderr(err_buf):
            code = args.func(args)
        return code, out_buf.getvalue(), err_buf.getvalue()

    # ---- 解析与旧命令兼容 ------------------------------------------------
    def test_parser_has_new_subcommands_and_legacy_remain(self):
        # 新/旧子命令都存在且缺必填参数按 usage 退出（control 无必填）
        for sub in ("pause", "resume", "hold", "turn-begin", "turn-finish",
                    "block", "unlock"):
            err = StringIO()
            with redirect_stderr(err):
                with self.assertRaises(SystemExit) as cm:
                    self._parse(["coordinator", sub])
            self.assertEqual(cm.exception.code, 2, sub)
            self.assertIn("required", err.getvalue(),
                          f"{sub} 缺必填参数按 usage 退出")
        args = self._parse(["coordinator", "control"])
        self.assertEqual(args.command, "coordinator")
        self.assertEqual(args.action, "control")

    # ---- pause/resume：显式绑定 + CAS 转发 ------------------------------
    def test_pause_forwards_explicit_binding_and_cas(self):
        code, out, err = self._exec(
            ["coordinator", "pause", "--project", "/p", "--session", "ses_c",
             "--operation-id", "op-orig", "--reason", "user-stop",
             "--expected-revision", "3", "--expected-epoch", "2",
             "--evidence", '{"refs":["ref-1","ref-2"]}'])
        self.assertEqual(code, 0, out + err)
        self.assertIn("仅阻新自动派单/协调唤醒", out,
                      "暂停范围必须明示不取消在途 worker")
        rec = self.fake.calls[-1]
        self.assertEqual(rec[0], "pause")
        self.assertEqual(rec[1], {
            "project": "/p", "session_id": "ses_c", "operation_id": "op-orig",
            "reason": "user-stop", "expected_revision": 3, "expected_epoch": 2,
            "evidence": {"refs": ["ref-1", "ref-2"]}})

    def test_pause_requires_operation_id_and_reason_explicitly(self):
        with self.assertRaises(SystemExit) as cm:
            self._parse(["coordinator", "pause", "--project", "/p",
                         "--session", "ses_c", "--reason", "r",
                         "--expected-revision", "3"])
        self.assertEqual(cm.exception.code, 2)
        with self.assertRaises(SystemExit) as cm:
            self._parse(["coordinator", "pause", "--project", "/p",
                         "--session", "ses_c", "--operation-id", "op",
                         "--expected-revision", "3"])
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(self.fake.calls, [], "缺必填参数不得触碰 service")

    def test_pause_evidence_non_object_rejected_by_cli(self):
        for bad in ("[1,2]", '"string"', "'scalar'", "not-json"):
            with self.subTest(evidence=bad):
                code, out, err = self._exec(
                    ["coordinator", "pause", "--project", "/p", "--session", "ses_c",
                     "--operation-id", "op", "--reason", "r",
                     "--expected-revision", "3", "--evidence", bad], as_json=True)
                self.assertEqual(code, 2, out + err)
                self.assertEqual(json.loads(out)["code"], "usage")
        # 合法 JSON 对象放行
        code, out, err = self._exec(
            ["coordinator", "pause", "--project", "/p", "--session", "ses_c",
             "--operation-id", "op", "--reason", "r",
             "--expected-revision", "3", "--evidence", '{"ref":"ok"}'], as_json=True)
        self.assertEqual(code, 0, out + err)

    def test_pause_missing_cas_rejected_by_cli(self):
        with self.assertRaises(SystemExit) as cm:
            self._parse(["coordinator", "pause", "--project", "/p",
                         "--session", "ses_c", "--operation-id", "op",
                         "--reason", "r"])
        self.assertEqual(cm.exception.code, 2, "CAS 不能默认省略")

    def test_resume_forwards_and_marks_scope(self):
        code, out, err = self._exec(
            ["coordinator", "resume", "--project", "/p", "--session", "ses_c",
             "--operation-id", "op-orig", "--reason", "user-continue",
             "--expected-revision", "3"])
        self.assertEqual(code, 0, out + err)
        self.assertEqual(self.fake.calls[-1][0], "resume")
        self.assertIn("不清 provider 冷却", out,
                      "继续不得暗含清除 provider/任务级门禁")

    def test_service_refusal_is_reported_not_oral_success(self):
        self.fake.mutation_result = {
            "executed": False, "refused": "cas_revision_mismatch",
            "refused_detail": {"expected": 3, "current": 5}}
        code, out, err = self._exec(
            ["coordinator", "pause", "--project", "/p", "--session", "ses_c",
             "--operation-id", "op", "--reason", "r",
             "--expected-revision", "3"], as_json=True)
        self.assertEqual(code, 1, out + err)
        payload = json.loads(out)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["code"], "control_refused")
        self.assertEqual(payload["detail"]["refused"], "cas_revision_mismatch")
        self.assertNotIn('"executed": true', out)

    def test_wrong_holder_rejected_before_service(self):
        code, out, err = self._exec(
            ["coordinator", "pause", "--project", "/p", "--session", "ses_other",
             "--operation-id", "op", "--reason", "r",
             "--expected-revision", "3"], as_json=True)
        self.assertEqual(code, 2, out + err)  # usage
        self.assertEqual(json.loads(out)["code"], "usage")
        self.assertNotIn("pause", [c[0] for c in self.fake.calls],
                         "错 holder 不得调用写方法")

    # ---- hold / user turn ----------------------------------------------
    def test_hold_forwards_session_event_and_reason(self):
        code, out, err = self._exec(
            ["coordinator", "hold", "--session", "ses_c",
             "--event-id", "evt-stop", "--origin-at", "9000000000",
             "--reason", "user-stopped-answer"])
        self.assertEqual(code, 0, out + err)
        self.assertIn("自动 wake 不自行解除", out)
        rec = self.fake.calls[-1]
        self.assertEqual(rec[0], "hold_session")
        self.assertEqual(rec[1], {"session_id": "ses_c",
                                  "event_id": "evt-stop",
                                  "origin_at": 9_000_000_000,
                                  "reason": "user-stopped-answer"})

    def test_turn_begin_requires_explicit_trusted(self):
        # trusted=False / 缺省一律不建立“屏障已建立”假象：CLI 层直接按 usage 拒。
        with self.assertRaises(SystemExit) as cm:
            self._parse(["coordinator", "turn-begin", "--session", "ses_c",
                         "--message-id", "msg-u1", "--origin-at", "9000000000",
                         "--operation-id", "op-u1"])
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(self.fake.calls, [], "无 --trusted 不得触碰 service")

    def test_turn_begin_explicit_trusted_flag(self):
        code, out, err = self._exec(
            ["coordinator", "turn-begin", "--session", "ses_c",
             "--message-id", "msg-u1", "--origin-at", "9000000000",
             "--operation-id", "op-u1", "--trusted"])
        self.assertEqual(code, 0, out + err)
        self.assertIs(self.fake.calls[-1][1]["trusted"], True)
        self.assertIn("回合屏障已建立（显式可信判定）", out)

    def test_turn_finish_requires_full_idle_evidence_and_cas(self):
        code, out, err = self._exec(
            ["coordinator", "turn-finish", "--session", "ses_c",
             "--message-id", "msg-u1", "--idle-id", "idl-1",
             "--idle-at", "9000000100", "--expected-revision", "3"])
        self.assertEqual(code, 0, out + err)
        self.assertIn("本地 operator 断言已登记", out)
        rec = self.fake.calls[-1]
        self.assertEqual(rec[0], "finish_user_turn")
        self.assertEqual(rec[1], {"session_id": "ses_c", "message_id": "msg-u1",
                                  "idle_id": "idl-1", "idle_at": 9_000_000_100,
                                  "expected_revision": 3})

    def test_turn_finish_missing_idle_evidence_is_usage(self):
        # 没有「空 inbox/updated 即完成」：缺结束证据直接按 usage 拒绝
        with self.assertRaises(SystemExit) as cm:
            self._parse(["coordinator", "turn-finish", "--session", "ses_c",
                         "--message-id", "msg-u1", "--idle-at", "1",
                         "--expected-revision", "3"])
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(self.fake.calls, [])

    # ---- control：只读状态 ----------------------------------------------
    def test_control_reads_project_session_and_permits(self):
        code, out, err = self._exec(
            ["coordinator", "control", "--project", "/p", "--session", "ses_c"],
            as_json=True)
        self.assertEqual(code, 0, out + err)
        payload = json.loads(out)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["project"]["revision"], 3)
        self.assertEqual(payload["session"]["session_id"], "ses_c")
        self.assertIs(payload["permits"]["permits"], False)
        self.assertEqual([c[0] for c in self.fake.calls],
                         ["get_project", "get_session", "permits"],
                         "control 不得触发写方法")

    def test_pause_rejects_negative_and_zero_epoch_cas(self):
        # 负 revision/负或零 epoch 一律 usage（int 已解析，用例层 fail-closed）
        base = ["coordinator", "pause", "--project", "/p", "--session", "ses_c",
                "--operation-id", "op", "--reason", "r"]
        cases = (["--expected-revision", "-1"],
                 ["--expected-revision", "3", "--expected-epoch", "0"],
                 ["--expected-revision", "3", "--expected-epoch", "-2"])
        for extra in cases:
            with self.subTest(extra=extra):
                code, out, err = self._exec(base + extra, as_json=True)
                self.assertEqual(code, 2, out + err)
                self.assertEqual(json.loads(out)["code"], "usage")

    def test_control_requires_at_least_one_target(self):
        code, out, err = self._exec(["coordinator", "control"], as_json=True)
        self.assertEqual(code, 2, out + err)
        self.assertEqual(json.loads(out)["code"], "usage")

    def test_control_human_marks_readonly_view(self):
        code, out, err = self._exec(
            ["coordinator", "control", "--project", "/p", "--session", "ses_c"])
        self.assertEqual(code, 0, out + err)
        self.assertIn("只读展示", out)
        self.assertIn("permits=no", out)

    # ---- 旧 block/unlock 仍可用（真实临时台账，不触网） -------------------
    def _exec_home(self, home, argv):
        raw = ["--home", home] + argv
        args = cli.build_parser().parse_args(raw)
        out_buf, err_buf = StringIO(), StringIO()
        with redirect_stdout(out_buf), redirect_stderr(err_buf):
            code = args.func(args)
        return code, out_buf.getvalue(), err_buf.getvalue()

    def test_legacy_block_still_works_with_real_ledger(self):
        from oc_orch.adapters.sqlite_db import init
        from oc_orch.adapters.sqlite_ledger import Ledger
        home = tempfile.mkdtemp(prefix="coord-ctrl-legacy-", dir="/tmp/opencode")
        self.addCleanup(lambda: shutil.rmtree(home, ignore_errors=True))
        conn = init(home)
        self.addCleanup(conn.close)
        ledger = Ledger(conn, home)
        ledger.claim_coordinator(project="/p-legacy", session_id="ses_legacy",
                                 actor="test")
        code, out, err = self._exec_home(
            home, ["coordinator", "block", "--project", "/p-legacy",
                   "--reason", "operator-pause"])
        self.assertEqual(code, 0, out + err)
        # 旧 block 仍写 target-gate（legacy 兼容）；新 pause 走共享控制用例
        self.assertIn("blocked coordinator", out)



# ---- 真实临时 SQLite CLI 对接（不注入替身，走 composition factory） ----
# 经临时 OC_ORCH_HOME + 真实 claim_coordinator + make_coordinator_control，
# 不触网、不读生产服务。验证真实返回 shape（pause/resume 顶层 revision/epoch、
# snapshot.status）、CAS 拒绝如实上报、租约交接拒绝与 finish 不清 provider 门。
_PROVIDER_CLASSIFICATION = {
    "category": "quota", "transient": False, "status": 429,
    "message": "limit", "action": "block-and-alert",
    "limit_kind": "subscription", "code": "provider.limit",
}


class RealLedgerCoordinatorControlCliTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="coord-ctrl-real-cli-",
                                    dir="/tmp/opencode")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        self.conn = init(self.tmp)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.tmp)
        self.clock = SystemClock()

    def _claim(self, project, session_id, *, ttl=3600, **kw):
        return self.ledger.claim_coordinator(
            project=project, session_id=session_id, actor="test", ttl=ttl, **kw)

    def _cli(self, argv, as_json=True):
        raw = (["--json"] if as_json else []) + ["--home", self.tmp] + list(argv)
        args = cli.build_parser().parse_args(raw)
        out_buf, err_buf = StringIO(), StringIO()
        # 不 patch _open_coordinator_control：真实 composition factory
        with redirect_stdout(out_buf), redirect_stderr(err_buf):
            code = args.func(args)
        return code, out_buf.getvalue(), err_buf.getvalue()

    def test_real_pause_resume_flow_via_cli(self):
        self._claim("/p", "ses_c")
        code, out, err = self._cli(
            ["coordinator", "pause", "--project", "/p", "--session", "ses_c",
             "--operation-id", "op-1", "--reason", "explicit-stop",
             "--expected-revision", "0", "--expected-epoch", "1"])
        self.assertEqual(code, 0, out + err)
        payload = json.loads(out)
        self.assertTrue(payload["executed"])
        self.assertEqual(payload["result"]["applied"], True)
        self.assertEqual(payload["result"]["revision"], 1)
        self.assertEqual(payload["result"]["epoch"], 1, "A 顶层 epoch 必须返回")
        self.assertEqual(payload["result"]["snapshot"]["status"], "paused")
        self.assertEqual(payload["result"]["snapshot"]["operation_id"], "op-1")

        code, out, err = self._cli(
            ["coordinator", "resume", "--project", "/p", "--session", "ses_c",
             "--operation-id", "op-2", "--reason", "explicit-go",
             "--expected-revision", "1", "--expected-epoch", "1"])
        self.assertEqual(code, 0, out + err)
        payload = json.loads(out)
        self.assertTrue(payload["executed"])
        self.assertEqual(payload["result"]["revision"], 2)
        self.assertEqual(payload["result"]["snapshot"]["status"], "running")

    def test_real_stale_cas_reported_not_success(self):
        self._claim("/p", "ses_c")
        self.assertEqual(self._cli(["coordinator", "pause", "--project", "/p",
                                    "--session", "ses_c", "--operation-id", "op-1",
                                    "--reason", "r", "--expected-revision", "0"])[0], 0)
        self.assertEqual(self._cli(["coordinator", "resume", "--project", "/p",
                                    "--session", "ses_c", "--operation-id", "op-2",
                                    "--reason", "r", "--expected-revision", "1"])[0], 0)
        # 旧 CAS 重放：must 如实报未执行，不得显示已暂停成功
        code, out, err = self._cli(
            ["coordinator", "pause", "--project", "/p", "--session", "ses_c",
             "--operation-id", "op-3", "--reason", "r",
             "--expected-revision", "0"])
        self.assertEqual(code, 1, out + err)
        payload = json.loads(out)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["code"], "control_refused")
        self.assertEqual(payload["detail"]["refused"], "revision-changed")
        self.assertNotIn('"executed": true', out)
        # 状态保持 running / revision 2（被拒后无新事实改写）
        ctrl = __import__("oc_orch.composition", fromlist=["make_coordinator_control"])
        svc = ctrl.make_coordinator_control(self.conn, SystemClock())
        self.assertEqual(svc.get_project("/p")["status"], "running")
        self.assertEqual(svc.get_project("/p")["revision"], 2)

    def test_real_handover_rejects_old_holder_and_accepts_new(self):
        self._claim("/p", "ses_a")
        self._claim("/p", "ses_b", takeover=True, old_confirmed_stopped=True)
        self.assertEqual(self.ledger.coordinator_lease("/p")["epoch"], 2)
        # 旧 holder：入口 holder 预检以 usage 拒绝（不冒进 service）
        code, out, err = self._cli(
            ["coordinator", "pause", "--project", "/p", "--session", "ses_a",
             "--operation-id", "opA", "--reason", "r",
             "--expected-revision", "0", "--expected-epoch", "1"])
        self.assertEqual(code, 2, out + err)
        self.assertEqual(json.loads(out)["code"], "usage")
        self.assertIn("不是项目 /p 的当前协调者", out)
        # 新 holder 配错代次 → epoch-mismatch（service 拒绝）
        code, out, err = self._cli(
            ["coordinator", "pause", "--project", "/p", "--session", "ses_b",
             "--operation-id", "opB", "--reason", "r",
             "--expected-revision", "0", "--expected-epoch", "1"])
        self.assertEqual(code, 1, out + err)
        self.assertEqual(json.loads(out)["detail"]["refused"], "epoch-mismatch")
        # 新 holder 正确代次 → 成功
        code, out, err = self._cli(
            ["coordinator", "pause", "--project", "/p", "--session", "ses_b",
             "--operation-id", "opB", "--reason", "r",
             "--expected-revision", "0", "--expected-epoch", "2"])
        self.assertEqual(code, 0, out + err)
        self.assertTrue(json.loads(out)["executed"])

    def test_real_turn_begin_finish_keeps_provider_gate(self):
        self._claim("/p", "ses_c")
        gate = TargetGate(self.conn, self.clock)
        t0 = self.clock.now_ms()
        gate.observe_error("ses_c", dict(_PROVIDER_CLASSIFICATION),
                           evidence_key="cli:gate1", observed_at=t0)
        self.assertEqual(gate.get("ses_c")["source"], "provider")
        # turn-begin 必须 --trusted（这里走真实路径）
        code, out, err = self._cli(
            ["coordinator", "turn-begin", "--session", "ses_c",
             "--message-id", "msg-1", "--origin-at", str(t0 + 1000),
             "--operation-id", "op-1", "--trusted"])
        self.assertEqual(code, 0, out + err)
        self.assertTrue(json.loads(out)["executed"])
        code, out, err = self._cli(
            ["coordinator", "turn-finish", "--session", "ses_c",
             "--message-id", "msg-1", "--idle-id", "idl-1",
             "--idle-at", str(t0 + 2000), "--expected-revision", "1"])
        self.assertEqual(code, 0, out + err)
        payload = json.loads(out)
        self.assertTrue(payload["executed"])
        # operator 断言不清 provider gate
        g = gate.get("ses_c")
        self.assertIsNotNone(g)
        self.assertEqual(g["source"], "provider")

    def test_real_history_replay_human_does_not_claim_stale_state(self):
        # pause(op1)->resume(op2) 后重放旧 op1：history 幂等，本次未变更；
        # human 不得再声称 paused，JSON executed 表示历史已处理、快照保持真实。
        self._claim("/p", "ses_c")
        self.assertEqual(self._cli(["coordinator", "pause", "--project", "/p",
                                    "--session", "ses_c", "--operation-id", "op-1",
                                    "--reason", "r", "--expected-revision", "0"])[0], 0)
        self.assertEqual(self._cli(["coordinator", "resume", "--project", "/p",
                                    "--session", "ses_c", "--operation-id", "op-2",
                                    "--reason", "r", "--expected-revision", "1"])[0], 0)

        code, out, err = self._cli(
            ["coordinator", "pause", "--project", "/p", "--session", "ses_c",
             "--operation-id", "op-1", "--reason", "r", "--expected-revision", "2"],
            as_json=False)
        self.assertEqual(code, 0, out + err)
        self.assertNotIn("paused project=/p", out, "历史重放不得宣称当前已暂停")
        self.assertIn("已处理但本次未变更", out)
        self.assertIn("applied=false", out)
        self.assertIn("status=running", out)
        self.assertIn("revision=2", out)

        code, out, err = self._cli(
            ["coordinator", "pause", "--project", "/p", "--session", "ses_c",
             "--operation-id", "op-1", "--reason", "r", "--expected-revision", "2"])
        self.assertEqual(code, 0, out + err)
        payload = json.loads(out)
        self.assertTrue(payload["executed"], "历史已处理操作仍 executed=True")
        self.assertFalse(payload["result"]["applied"])
        self.assertEqual(payload["result"]["snapshot"]["status"], "running")
        self.assertEqual(payload["result"]["snapshot"]["revision"], 2)
        self.assertEqual(payload["result"]["revision"], 2)

    def test_real_control_status_shows_revision_epoch_and_projects(self):
        self._claim("/p1", "ses_c")
        code, out, err = self._cli(
            ["coordinator", "control", "--project", "/p1", "--session", "ses_c"])
        self.assertEqual(code, 0, out + err)
        payload = json.loads(out)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["project"]["revision"], 0)
        self.assertEqual(payload["project"]["epoch"], 1)
        self.assertIn("/p1", payload["session"]["projects"])
        self.assertTrue(payload["permits"]["permits"])

if __name__ == "__main__":
    unittest.main(verbosity=2)
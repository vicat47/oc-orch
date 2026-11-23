#!/usr/bin/env python3
"""对话控制可信入口边界（spec C01–C04；任务 0.2/1.1）。

被测对象是 application.conversation_control.ConversationControl —— 显式
operator 动作 → CoordinatorControlPort 调用的 fail-closed 共享用例。Coordinator
ControlService（worker A）尚在父集成，故用离线替身按协议实现；本文件不触网、
不读生产台账、不调用真实 CLI。

关键断言：
- 进入字段（project/session/operation-id/reason/CAS/event/message/idle）缺任一
  即拒，service 不被调用；
- CAS 是硬约束：expected-revision 缺失/类型错不调用 service；
- 来源认证不是 metadata 字面值：自称 user/automatic_wake/未登记 REST 输入均拒；
- 多项目歧义、无归属项目、错 holder 均拒，不猜；
- service 返回 executed=False 时如实报「未执行」，不口头成功；
- finish-user-turn 必须携带原输入精确关联 + 可信结束证据 + CAS；
  无「空 inbox/updated 即算结束」捷径。
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from oc_orch.application.conversation_control import (  # noqa: E402
    ATTESTATION_OPERATOR,
    ConversationControl,
)
from oc_orch.adapters.clock import SystemClock  # noqa: E402
from oc_orch.adapters.sqlite_db import init  # noqa: E402
from oc_orch.adapters.sqlite_ledger import Ledger  # noqa: E402
from oc_orch.adapters.target_gate import TargetGate  # noqa: E402
from oc_orch.composition import make_coordinator_control  # noqa: E402
from oc_orch.domain.errors import ActionError  # noqa: E402


class _FakeControl:
    """CoordinatorControlPort 离线替身：记录调用，状态可布置。"""

    def __init__(self):
        self.calls = []
        self.projects = {}
        self.sessions = {}
        self.mutation_result = {"executed": True, "changed": True,
                                "revision": 3, "epoch": 2}

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
        return self.mutation_result

    def resume(self, project, **kw):
        self._rec("resume", project=project, **kw)
        return self.mutation_result

    def hold_session(self, session_id, **kw):
        self._rec("hold_session", session_id=session_id, **kw)
        return self.mutation_result

    def begin_user_turn(self, session_id, **kw):
        self._rec("begin_user_turn", session_id=session_id, **kw)
        return self.mutation_result

    def finish_user_turn(self, session_id, **kw):
        self._rec("finish_user_turn", session_id=session_id, **kw)
        return self.mutation_result

    def permits(self, project, session_id):
        self._rec("permits", project=project, session_id=session_id)
        return {"permits": False, "blocking": ["pause"]}


def _project_intent(project, holder, *, revision=3, epoch=2, paused=True,
                    operation_id="op-orig", reason="user-requested"):
    return {
        "project": project, "holder_session": holder, "epoch": epoch,
        "revision": revision, "paused": paused,
        "pause": (None if not paused else {
            "operation_id": operation_id, "reason": reason,
            "requested_by": ATTESTATION_OPERATOR, "requested_at": 1_000}),
    }


def _session_view(session_id, projects, *, hold=None, turn=None):
    return {"session_id": session_id, "projects": list(projects),
            "hold": hold, "turn": turn}


class ConversationControlEntryTests(unittest.TestCase):
    def setUp(self):
        self.fake = _FakeControl()
        self.use = ConversationControl(self.fake)
        self.base = dict(
            project="/p", session_id="ses_c", operation_id="op-orig",
            reason="explicit-stop", expected_revision=3)

    def _call(self, method, **overrides):
        kw = dict(self.base)
        kw.update(overrides)
        return getattr(self.use, method)(**kw)

    def test_pause_forwards_exact_params_and_reports_executed(self):
        out = self.use.pause(**self.base, expected_epoch=2,
                             evidence={"ref": "audit-ref"})
        self.assertTrue(out["executed"])
        self.assertEqual(out["project"], "/p")
        self.assertEqual(out["session_id"], "ses_c")
        self.assertEqual(out["result"]["revision"], 3)
        rec = self.fake.calls[-1]
        self.assertEqual(rec[0], "pause")
        _, kw = rec
        self.assertEqual(kw, {"project": "/p", "session_id": "ses_c",
                              "operation_id": "op-orig",
                              "reason": "explicit-stop",
                              "expected_revision": 3, "expected_epoch": 2,
                              "evidence": {"ref": "audit-ref"}})

    def test_resume_idempotent_executed_true_is_ok(self):
        self.fake.mutation_result = {"executed": True, "changed": False,
                                     "revision": 3, "epoch": 2}
        out = self.use.resume(**self.base)
        self.assertTrue(out["executed"])
        self.assertTrue(out["result"]["changed"] is False)
        self.assertEqual(self.fake.calls[-1][0], "resume")

    def test_pause_evidence_is_json_object_and_passed_through(self):
        # 对齐 worker A 真实 API：evidence 为 dict（JSON 对象），原样传给
        # service 规范形化；list/str 等非对象一律拒绝（不 str() 隐式放行）。
        self.use.pause(**self.base, evidence={"refs": ["ref-1", 7]})
        _, kw = self.fake.calls[-1]
        self.assertEqual(kw["evidence"], {"refs": ["ref-1", 7]})
        self.fake.calls = []
        for bogus in (["ref-1"], "ref-1", 7, b"x"):
            with self.subTest(evidence=type(bogus).__name__):
                with self.assertRaises(ActionError) as cm:
                    self._call("pause", evidence=bogus)
                self.assertEqual(cm.exception.code, "usage")
                self.assertEqual(self.fake.calls, [])

    def test_pause_missing_evidence_passed_as_none(self):
        self.use.pause(**self.base, evidence=None)
        _, kw = self.fake.calls[-1]
        self.assertIsNone(kw["evidence"])

    # ---- CAS / 字段硬约束：缺任一即拒且不调用 service -----------------
    def test_pause_missing_expected_revision_refused_before_service(self):
        with self.assertRaises(ActionError) as cm:
            self._call("pause", expected_revision=None)
        self.assertEqual(cm.exception.code, "usage")
        self.assertEqual(self.fake.calls, [], "CAS 缺失不得触碰 service")

    def test_pause_non_int_expected_revision_refused(self):
        with self.assertRaises(ActionError) as cm:
            self._call("pause", expected_revision="3")
        self.assertEqual(cm.exception.code, "usage")
        self.assertEqual(self.fake.calls, [])

    def test_pause_missing_operation_id_refused_before_service(self):
        with self.assertRaises(ActionError) as cm:
            self._call("pause", operation_id=None)
        self.assertEqual(cm.exception.code, "usage")
        self.assertEqual(self.fake.calls, [])

    def test_pause_blank_reason_refused_before_service(self):
        with self.assertRaises(ActionError) as cm:
            self._call("pause", reason="   ")
        self.assertEqual(cm.exception.code, "usage")
        self.assertEqual(self.fake.calls, [])

    def test_pause_missing_session_refused_before_service(self):
        with self.assertRaises(ActionError) as cm:
            self._call("pause", session_id=None)
        self.assertEqual(cm.exception.code, "usage")
        self.assertEqual(self.fake.calls, [])

    def test_pause_non_int_expected_epoch_refused_before_service(self):
        with self.assertRaises(ActionError) as cm:
            self._call("pause", expected_epoch="2")
        self.assertEqual(cm.exception.code, "usage")
        self.assertEqual(self.fake.calls, [])

    # ---- C03：metadata 自称不是来源认证 -------------------------------
    def test_attestation_user_metadata_is_never_accepted(self):
        for bogus in ("user-metadata", "automatic_wake",
                      "rest-unregistered", "session.inbox.enqueued"):
            with self.subTest(source=bogus):
                with self.assertRaises(ActionError) as cm:
                    self._call("pause", attestation=bogus)
                self.assertEqual(cm.exception.code, "usage")
                self.assertEqual(self.fake.calls, [],
                                 f"{bogus} 不得取得控制权")

    def test_operator_attestation_is_the_only_accepted_value(self):
        self.use.pause(**self.base)
        self.assertEqual(self.fake.calls[-1][0], "pause")

    # ---- C04：歧义/归属不猜 -------------------------------------------
    def test_multi_project_ambiguity_refused(self):
        self.fake.sessions["ses_c"] = _session_view(
            "ses_c", ["/p1", "/p2"])
        with self.assertRaises(ActionError) as cm:
            self.use.pause(project=None, session_id="ses_c",
                           operation_id="op", reason="r",
                           expected_revision=1)
        self.assertEqual(cm.exception.code, "usage")
        self.assertIn("多个项目", cm.exception.message)
        self.assertEqual(self.fake.calls, [("get_session", {"session_id": "ses_c"})],
                         "歧义时不得调用写方法")

    def test_no_attributable_project_refused(self):
        self.fake.sessions["ses_c"] = _session_view("ses_c", [])
        with self.assertRaises(ActionError) as cm:
            self.use.resume(project=None, session_id="ses_c",
                            operation_id="op", reason="r",
                            expected_revision=1)
        self.assertEqual(cm.exception.code, "usage")
        self.assertEqual(self.fake.calls, [("get_session", {"session_id": "ses_c"})])

    def test_single_project_resolution_without_explicit_project(self):
        self.fake.sessions["ses_c"] = _session_view("ses_c", ["/p"])
        self.fake.projects["/p"] = _project_intent("/p", "ses_c")
        self.use.pause(project=None, session_id="ses_c",
                       operation_id="op", reason="r", expected_revision=2)
        call = self.fake.calls[-1]
        self.assertEqual(call[0], "pause")
        self.assertEqual(call[1]["project"], "/p")

    def test_wrong_holder_refused(self):
        self.fake.projects["/p"] = _project_intent("/p", "ses_other")
        with self.assertRaises(ActionError) as cm:
            self._call("pause")
        self.assertEqual(cm.exception.code, "usage")
        self.assertIn("不是项目 /p 的当前协调者", cm.exception.message)
        # holder 预检失败：不得调用 pause 写方法
        self.assertEqual([c[0] for c in self.fake.calls], ["get_project"])

    def test_no_control_record_holder_check_is_best_effort(self):
        # get_project 尚无记录：交由 service 负责创建/CAS，入口不猜 holder
        self.use.pause(**self.base)
        self.assertEqual(self.fake.calls[-1][0], "pause")

    # ---- service 拒绝必须如实报未执行 ---------------------------------
    def test_service_refused_is_not_oral_success(self):
        self.fake.mutation_result = {
            "executed": False, "refused": "cas_revision_mismatch",
            "refused_detail": {"expected": 3, "current": 4}}
        with self.assertRaises(ActionError) as cm:
            self._call("pause")
        self.assertEqual(cm.exception.code, "control_refused")
        self.assertIn("未执行", cm.exception.message)
        self.assertEqual(cm.exception.detail["refused"],
                         "cas_revision_mismatch")

    def test_service_nondict_result_refused(self):
        self.fake.mutation_result = "bogus"
        with self.assertRaises(ActionError) as cm:
            self._call("resume")
        self.assertEqual(cm.exception.code, "control_refused")

    # ---- hold：回合级，不取消项目/worker -------------------------------
    def test_hold_forwards_exact_params(self):
        self.use.hold(session_id="ses_c", event_id="evt-interrupt",
                      origin_at=9_000_000_000, reason="user-stop-answer")
        self.assertEqual(self.fake.calls[-1], (
            "hold_session",
            {"session_id": "ses_c", "event_id": "evt-interrupt",
             "origin_at": 9_000_000_000, "reason": "user-stop-answer"}))

    def test_hold_requires_event_id_and_origin_at(self):
        cases = ({"event_id": None}, {"origin_at": "now"},
                 {"origin_at": None}, {"reason": ""})
        for overrides in cases:
            with self.subTest(overrides=overrides):
                kw = dict(session_id="ses_c", event_id="evt-1",
                          origin_at=1, reason="r")
                kw.update(overrides)
                with self.assertRaises(ActionError) as cm:
                    self.use.hold(**kw)
                self.assertEqual(cm.exception.code, "usage")
                self.assertEqual(self.fake.calls, [],
                                 "缺字段不得调用 service")

    # ---- begin/finish user turn ----------------------------------------
    def test_begin_user_turn_without_trust_is_not_executed(self):
        # trusted=False（缺省）明确未执行：不建立“屏障已建立”的假象，也
        # 不因 fake service 默认 executed=True 就视为成功登记。
        with self.assertRaises(ActionError) as cm:
            self.use.begin_user_turn(session_id="ses_c", message_id="msg-u1",
                                     origin_at=9_000_000_000,
                                     operation_id="op-u1")
        self.assertEqual(cm.exception.code, "control_refused")
        self.assertIn("未执行", cm.exception.message)
        self.assertIn("缺少可信来源", cm.exception.message)
        self.assertEqual(self.fake.calls, [],
                         "trusted=False 不得调用 service，更不得显示屏障已建立")

    def test_begin_user_turn_explicit_trusted(self):
        self.use.begin_user_turn(session_id="ses_c", message_id="msg-u1",
                                 origin_at=9_000_000_000,
                                 operation_id="op-u1", trusted=True)
        kw = self.fake.calls[-1][1]
        self.assertTrue(kw["trusted"])
        self.assertEqual(kw["operation_id"], "op-u1")

    def test_begin_user_turn_rejects_non_bool_trusted(self):
        # bool('false')/1 等隐式转换会伪造“可信”，必须严格拒绝字面量。
        for bogus in ("false", "true", "1", 1, 0, None):
            with self.subTest(trusted=bogus):
                with self.assertRaises(ActionError) as cm:
                    self.use.begin_user_turn(session_id="ses_c",
                                             message_id="msg-u1",
                                             origin_at=1,
                                             operation_id="op-u1",
                                             trusted=bogus)
                self.assertEqual(cm.exception.code, "usage")
                self.assertEqual(self.fake.calls, [],
                                 f"trusted={bogus!r} 不得调用 service")

    def test_core_str_fields_require_real_nonempty_strings(self):
        # project/session/operation/reason 必须真实字符串；int/list/bytes 等
        # 不得被 str() 静默放行。
        cases = [
            ("project", 123), ("session_id", 7),
            ("operation_id", ["x"]), ("reason", 123),
            ("session_id", b"ses"), ("operation_id", {"k": "v"}),
        ]
        for field, value in cases:
            with self.subTest(field=field, value=type(value).__name__):
                with self.assertRaises(ActionError) as cm:
                    self._call("pause", **{field: value})
                self.assertEqual(cm.exception.code, "usage")
                self.assertEqual(self.fake.calls, [],
                                 f"{field}={type(value).__name__} 不得被 str() 放行")

    def test_int_fields_reject_bool_negative_and_epoch_below_one(self):
        for label, kwargs in (
                ("expected_revision=-1", {"expected_revision": -1}),
                ("expected_revision=True", {"expected_revision": True}),
                ("expected_epoch=-1", {"expected_epoch": -1}),
                ("expected_epoch=0", {"expected_epoch": 0})):
            with self.subTest(label=label):
                with self.assertRaises(ActionError) as cm:
                    self._call("pause", **kwargs)
                self.assertEqual(cm.exception.code, "usage")
                self.assertEqual(self.fake.calls, [])
        with self.assertRaises(ActionError) as cm:
            self.use.hold(session_id="ses_c", event_id="evt-1",
                          origin_at=-5, reason="r")
        self.assertEqual(cm.exception.code, "usage")
        with self.assertRaises(ActionError) as cm:
            self.use.finish_user_turn(session_id="ses_c", message_id="m",
                                      idle_id="i", idle_at=-3,
                                      expected_revision=1)
        self.assertEqual(cm.exception.code, "usage")
        self.assertEqual(self.fake.calls, [])

    def test_revision_zero_and_epoch_one_are_valid_boundaries(self):
        self.fake.mutation_result = {"executed": True, "changed": False,
                                     "revision": 0, "epoch": 1}
        out = self.use.resume(project="/p", session_id="ses_c",
                              operation_id="op", reason="r",
                              expected_revision=0, expected_epoch=1)
        self.assertTrue(out["executed"])
        self.assertEqual(self.fake.calls[-1][0], "resume")

    def test_begin_user_turn_missing_fields_refused(self):
        for overrides in ({"message_id": None}, {"origin_at": "x"},
                          {"operation_id": ""}):
            with self.subTest(overrides=overrides):
                kw = dict(session_id="ses_c", message_id="m", origin_at=1,
                          operation_id="o")
                kw.update(overrides)
                with self.assertRaises(ActionError) as cm:
                    self.use.begin_user_turn(**kw)
                self.assertEqual(cm.exception.code, "usage")
                self.assertEqual(self.fake.calls, [])

    def test_finish_user_turn_requires_full_evidence_and_cas(self):
        self.use.finish_user_turn(
            session_id="ses_c", message_id="msg-u1", idle_id="idl-1",
            idle_at=9_000_000_100, expected_revision=3)
        self.assertEqual(self.fake.calls[-1], (
            "finish_user_turn",
            {"session_id": "ses_c", "message_id": "msg-u1",
             "idle_id": "idl-1", "idle_at": 9_000_000_100,
             "expected_revision": 3}))

    def test_finish_user_turn_no_empty_inbox_shortcut(self):
        # 没有「空 inbox/updated 即完成」：缺结束证据或 CAS 一律拒绝，
        # 且不调用 service（fail-closed 接口本身不含该捷径）。
        cases = ({"idle_id": None}, {"idle_at": None},
                 {"expected_revision": None}, {"message_id": None})
        for overrides in cases:
            with self.subTest(overrides=overrides):
                kw = dict(session_id="ses_c", message_id="msg-u1",
                          idle_id="idl-1", idle_at=1, expected_revision=1)
                kw.update(overrides)
                with self.assertRaises(ActionError) as cm:
                    self.use.finish_user_turn(**kw)
                self.assertEqual(cm.exception.code, "usage")
                self.assertEqual(self.fake.calls, [],
                                 "缺结束证据/CAS 不得调用 service")

    def test_finish_user_turn_refused_not_oral_success(self):
        self.fake.mutation_result = {
            "executed": False, "refused": "turn_version_moved"}
        with self.assertRaises(ActionError) as cm:
            self.use.finish_user_turn(
                session_id="ses_c", message_id="msg-u1", idle_id="idl-1",
                idle_at=1, expected_revision=1)
        self.assertEqual(cm.exception.code, "control_refused")

    # ---- status：只读，不写 --------------------------------------------
    def test_status_reads_project_session_and_permits_only(self):
        self.fake.projects["/p"] = _project_intent("/p", "ses_c")
        self.fake.sessions["ses_c"] = _session_view(
            "ses_c", ["/p"],
            hold={"event_id": "evt-1", "origin_at": 5, "reason": "r"},
            turn={"message_id": "msg-u1", "origin_at": 5,
                  "operation_id": "op-u1", "trusted": False, "active": True})
        out = self.use.status(project="/p", session_id="ses_c")
        self.assertEqual(out["project"]["revision"], 3)
        self.assertEqual(out["session"]["turn"]["message_id"], "msg-u1")
        self.assertIs(out["permits"]["permits"], False)
        self.assertEqual([c[0] for c in self.fake.calls],
                         ["get_project", "get_session", "permits"],
                         "status 不得触发任何写方法")

    def test_status_without_permits_pair(self):
        self.fake.projects["/p"] = _project_intent("/p", "ses_c")
        out = self.use.status(project="/p")
        self.assertIsNone(out["permits"])
        self.assertEqual([c[0] for c in self.fake.calls], ["get_project"])



# ---- 真实临时 SQLite + composition.make_coordinator_control 对接 ---------
# 用真 coordinator_leases（claim_coordinator）与真 meta 持久化；不触网、不读
# 生产台账。验证 A 的真实返回 shape（pause/resume 顶层 revision/epoch、
# get_project 快照、get_session.projects 归属、operation 历史索引幂等）以及
# ConversationControl 入口的租约/CAS/trusted/provider-gate 语义。
_PROVIDER_CLASSIFICATION = {
    "category": "quota", "transient": False, "status": 429,
    "message": "limit", "action": "block-and-alert",
    "limit_kind": "subscription", "code": "provider.limit",
}


class RealLedgerConversationControlTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="coord-ctrl-real-",
                                          dir="/tmp/opencode")
        self.addCleanup(tmp.cleanup)
        self.home = tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home)
        self.clock = SystemClock()
        self.control = make_coordinator_control(self.conn, self.clock)
        self.use = ConversationControl(self.control)

    def _claim(self, project, session_id, *, ttl=3600, **kw):
        return self.ledger.claim_coordinator(
            project=project, session_id=session_id, actor="test", ttl=ttl, **kw)

    # ---- pause/resume：真实 CAS + 顶层 epoch + operation 索引 ---------
    def test_pause_resume_cas_and_operation_history_over_real_ledger(self):
        self._claim("/p", "ses_c")
        epoch = self.ledger.coordinator_lease("/p")["epoch"]
        self.assertEqual(epoch, 1)

        out = self.use.pause(project="/p", session_id="ses_c", operation_id="op-1",
                             reason="explicit-stop", expected_revision=0,
                             expected_epoch=epoch, evidence={"ref": "audit"})
        self.assertTrue(out["executed"])
        self.assertEqual(out["result"]["applied"], True)
        self.assertEqual(out["result"]["revision"], 1)
        self.assertEqual(out["result"]["epoch"], epoch, "A 顶层 epoch 必须返回")
        self.assertEqual(out["result"]["snapshot"]["status"], "paused")
        self.assertEqual(out["result"]["snapshot"]["operation_id"], "op-1")
        self.assertIs(out["result"]["snapshot"]["paused"], True)
        self.assertEqual(out["result"]["snapshot"]["evidence"], {"ref": "audit"})

        # 幂等：同 operation 同参数（含 evidence，指纹一致）→ applied=False、
        # 状态不变；参数任一变化（含 evidence）即 operation-conflict。
        repeat = self.use.pause(project="/p", session_id="ses_c",
                                operation_id="op-1", reason="explicit-stop",
                                expected_revision=1, expected_epoch=epoch,
                                evidence={"ref": "audit"})
        self.assertTrue(repeat["executed"])
        self.assertFalse(repeat["result"]["applied"])
        self.assertEqual(repeat["result"]["reason"], "already-applied")
        self.assertEqual(repeat["result"]["revision"], 1)

        # 参数变更不得冒认重复（operation-conflict）：换 reason 或换 evidence 都算
        base_pause = dict(project="/p", session_id="ses_c", operation_id="op-1",
                          expected_revision=1, expected_epoch=epoch,
                          reason="explicit-stop")
        for overrides in ({"reason": "different-reason"},
                          {"evidence": {"ref": "other"}}):
            with self.subTest(overrides=overrides):
                kw = dict(base_pause)
                kw.update(overrides)
                with self.assertRaises(ActionError) as cm:
                    self.use.pause(**kw)
                self.assertEqual(cm.exception.detail["refused"],
                                 "operation-conflict")

        # resume：CAS + 幂等 + 只改项目维度
        resume = self.use.resume(project="/p", session_id="ses_c",
                                 operation_id="op-2", reason="explicit-go",
                                 expected_revision=1, expected_epoch=epoch)
        self.assertTrue(resume["executed"])
        self.assertTrue(resume["result"]["applied"])
        self.assertEqual(resume["result"]["revision"], 2)
        self.assertEqual(resume["result"]["epoch"], epoch)
        self.assertEqual(resume["result"]["snapshot"]["status"], "running")

        # 新连接重开读持久化（真实 SQLite，非内存）
        conn2 = init(self.home)
        self.addCleanup(conn2.close)
        ctrl2 = make_coordinator_control(conn2, SystemClock())
        snap2 = ctrl2.get_project("/p")
        self.assertEqual(snap2["revision"], 2)
        self.assertEqual(snap2["status"], "running")
        self.assertEqual(snap2["last_operation"], "resume")

    def test_stale_cas_revision_rejected_over_real_ledger(self):
        self._claim("/p", "ses_c")
        self.use.pause(project="/p", session_id="ses_c", operation_id="op-1",
                       reason="stop", expected_revision=0)
        with self.assertRaises(ActionError) as cm:
            self.use.resume(project="/p", session_id="ses_c", operation_id="op-3",
                            reason="stale", expected_revision=0)
        self.assertEqual(cm.exception.detail["refused"], "revision-changed")
        self.assertEqual(self.control.get_project("/p")["status"], "paused",
                         "CAS 拒绝不清新状态")

    def test_operation_replay_does_not_become_new_pause_over_real_ledger(self):
        self._claim("/p", "ses_c")
        self.use.pause(project="/p", session_id="ses_c", operation_id="op-1",
                       reason="stop", expected_revision=0)
        self.use.resume(project="/p", session_id="ses_c", operation_id="op-2",
                        reason="go", expected_revision=1)
        # 旧 op-1 重放：同指纹 → already-applied，绝不成为新暂停
        replay = self.use.pause(project="/p", session_id="ses_c",
                                operation_id="op-1", reason="stop",
                                expected_revision=2)
        self.assertTrue(replay["executed"])
        self.assertFalse(replay["result"]["applied"])
        self.assertEqual(replay["result"]["reason"], "already-applied")
        self.assertEqual(self.control.get_project("/p")["status"], "running",
                         "历史 op 重放不得重新暂停项目")

    # ---- 租约：停用 / 过期 / 交接 / epoch 失配 ------------------------
    def test_lease_recovery_blocked_expired_handover_real(self):
        self._claim("/p", "ses_a")
        # 停用（recovery_blocked）
        self.ledger.block_coordinator_recovery("/p", reason="operator")
        with self.assertRaises(ActionError) as cm:
            self.use.pause(project="/p", session_id="ses_a", operation_id="opA",
                           reason="r", expected_revision=0)
        self.assertEqual(cm.exception.detail["refused"], "lease-recovery-blocked")
        # 过期
        self.ledger.reset_coordinator_failure("/p", reason="test")
        with self.conn:
            self.conn.execute(
                "UPDATE coordinator_leases SET expires_at=? WHERE project=?",
                (self.clock.now_ms() - 1000, "/p"))
        with self.assertRaises(ActionError) as cm:
            self.use.pause(project="/p", session_id="ses_a", operation_id="opA",
                           reason="r", expected_revision=0)
        self.assertEqual(cm.exception.detail["refused"], "lease-expired")
        # 交接（epoch 2）
        self._claim("/p", "ses_b", takeover=True, old_confirmed_stopped=True)
        self.assertEqual(self.ledger.coordinator_lease("/p")["epoch"], 2)
        # service 层：旧 holder 返回 lease-holder-mismatch
        direct = self.control.pause(
            "/p", session_id="ses_a", operation_id="opA", reason="r",
            expected_revision=0, expected_epoch=1)
        self.assertFalse(direct["executed"])
        self.assertEqual(direct["refused"], "lease-holder-mismatch")
        # 入口层：错 holder 由 holder 预检以 usage 拒绝（不冒进 service）
        with self.assertRaises(ActionError) as cm:
            self.use.pause(project="/p", session_id="ses_a", operation_id="opA",
                           reason="r", expected_revision=0, expected_epoch=1)
        self.assertEqual(cm.exception.code, "usage")
        self.assertIn("不是项目 /p 的当前协调者", cm.exception.message)
        # 新 holder 配错代次 → epoch-mismatch（入口 holder 预检通过，service 拒绝）
        with self.assertRaises(ActionError) as cm:
            self.use.pause(project="/p", session_id="ses_b", operation_id="opB",
                           reason="r", expected_revision=0, expected_epoch=1)
        self.assertEqual(cm.exception.detail["refused"], "epoch-mismatch")
        out = self.use.pause(project="/p", session_id="ses_b", operation_id="opB",
                             reason="r", expected_revision=0, expected_epoch=2)
        self.assertTrue(out["executed"])

    # ---- 归属：唯一推断 / 多项目拒绝 / 无项目拒绝 ---------------------
    def test_unique_ownership_inference_and_multi_project_rejection_real(self):
        self._claim("/p1", "ses_s")
        self.assertIn("/p1", self.control.get_session("ses_s")["projects"])
        out = self.use.pause(project=None, session_id="ses_s", operation_id="op",
                             reason="r", expected_revision=0)
        self.assertTrue(out["executed"])
        self.assertEqual(out["project"], "/p1")
        # 多项目歧义
        self._claim("/p2", "ses_s")
        self.assertIn("/p2", self.control.get_session("ses_s")["projects"])
        with self.assertRaises(ActionError) as cm:
            self.use.pause(project=None, session_id="ses_s", operation_id="op2",
                           reason="r", expected_revision=1)
        self.assertEqual(cm.exception.code, "usage")
        self.assertIn("多个项目", cm.exception.message)
        # 无归属项目
        with self.assertRaises(ActionError) as cm:
            self.use.pause(project=None, session_id="ses_none",
                           operation_id="op", reason="r", expected_revision=0)
        self.assertEqual(cm.exception.code, "usage")

    # ---- begin 需 trusted；untrusted 不建立屏障 -----------------------
    def test_begin_requires_trusted_over_real_ledger(self):
        self._claim("/p", "ses_c")
        t0 = self.clock.now_ms()
        # 入口层：trusted=False → 未执行
        with self.assertRaises(ActionError) as cm:
            self.use.begin_user_turn(session_id="ses_c", message_id="msg-1",
                                     origin_at=t0 + 1000, operation_id="op-t")
        self.assertEqual(cm.exception.code, "control_refused")
        self.assertIsNone(self.control.get_session("ses_c")["barrier"])
        # 真实 service 层（A 契约）：非严格 True 也拒绝 untrusted
        direct = self.control.begin_user_turn(
            "ses_c", message_id="msg-1", origin_at=t0 + 1000,
            operation_id="op-t", trusted=False)
        self.assertFalse(direct["executed"])
        self.assertEqual(direct["refused"], "untrusted")
        # 显式 trusted=True 才建立屏障
        out = self.use.begin_user_turn(session_id="ses_c", message_id="msg-1",
                                       origin_at=t0 + 1000, operation_id="op-t",
                                       trusted=True)
        self.assertTrue(out["executed"] and out["result"]["applied"])
        barrier = self.control.get_session("ses_c")["barrier"]
        self.assertEqual(barrier["message_id"], "msg-1")
        self.assertIs(barrier["trusted"], True)

    # ---- finish：operator 断言；不清 provider gate ---------------------
    def test_finish_assertion_releases_new_turn_hold_but_keeps_provider_gate(self):
        self._claim("/p", "ses_c")
        gate = TargetGate(self.conn, self.clock)
        t0 = self.clock.now_ms()
        gate.observe_error("ses_c", dict(_PROVIDER_CLASSIFICATION),
                           evidence_key="real:gate1", observed_at=t0)
        self.assertEqual(gate.get("ses_c")["source"], "provider")

        self.use.hold(session_id="ses_c", event_id="evt-1", origin_at=t0 + 1000,
                      reason="user-stopped-answer")
        self.use.begin_user_turn(session_id="ses_c", message_id="msg-1",
                                 origin_at=t0 + 2000, operation_id="op-1",
                                 trusted=True)
        snap = self.control.get_session("ses_c")
        self.assertEqual(snap["revision"], 2)
        self.assertIsNotNone(snap["hold"])
        self.assertEqual(snap["barrier"]["message_id"], "msg-1")

        # 错误 message 不得结束
        with self.assertRaises(ActionError) as cm:
            self.use.finish_user_turn(session_id="ses_c", message_id="msg-other",
                                      idle_id="idl-1", idle_at=t0 + 3000,
                                      expected_revision=2)
        self.assertEqual(cm.exception.detail["refused"], "message-mismatch")
        # 正确结束：解除当前新回合的 hold + barrier
        out = self.use.finish_user_turn(session_id="ses_c", message_id="msg-1",
                                        idle_id="idl-1", idle_at=t0 + 3000,
                                        expected_revision=2)
        self.assertTrue(out["executed"] and out["result"]["applied"])
        snap2 = self.control.get_session("ses_c")
        self.assertIsNone(snap2["barrier"])
        self.assertIsNone(snap2["hold"])
        # provider gate 原样保留（本接口不动 target-gate 命名空间）
        g = gate.get("ses_c")
        self.assertIsNotNone(g)
        self.assertEqual(g["source"], "provider")
        # 审计同事务落库
        rows = self.conn.execute(
            "SELECT DISTINCT action FROM audit WHERE entity='session' AND "
            "entity_id=? AND action LIKE 'coordinator-control.%'",
            ("ses_c",)).fetchall()
        self.assertGreaterEqual(len(rows), 3)

    def test_finish_stale_termination_and_no_active_turn_real(self):
        self._claim("/p", "ses_c")
        t0 = self.clock.now_ms()
        with self.assertRaises(ActionError) as cm:
            self.use.finish_user_turn(session_id="ses_c", message_id="m",
                                      idle_id="i", idle_at=t0 + 100,
                                      expected_revision=0)
        self.assertEqual(cm.exception.detail["refused"], "no-active-turn")
        self.use.begin_user_turn(session_id="ses_c", message_id="m",
                                 origin_at=t0 + 100, operation_id="op",
                                 trusted=True)
        # 旧 idle（早于水位）不能清更新屏障
        with self.assertRaises(ActionError) as cm:
            self.use.finish_user_turn(session_id="ses_c", message_id="m",
                                      idle_id="i", idle_at=t0 + 50,
                                      expected_revision=1)
        self.assertEqual(cm.exception.detail["refused"], "stale-termination")
        self.assertIsNotNone(self.control.get_session("ses_c")["barrier"])

    # ---- permits 三维组合（必要不充分） -------------------------------
    def test_permits_reflects_project_and_session_conditions_real(self):
        self._claim("/p", "ses_c")
        t0 = self.clock.now_ms()
        p0 = self.control.permits("/p", "ses_c")
        self.assertTrue(p0["permits"])
        self.use.pause(project="/p", session_id="ses_c", operation_id="op-p",
                       reason="r", expected_revision=0)
        p1 = self.control.permits("/p", "ses_c")
        self.assertIn("project-paused", p1["blocking"])
        # 暂停期间建立可信用户回合 → 双维度阻塞
        self.use.begin_user_turn(session_id="ses_c", message_id="m",
                                 origin_at=t0 + 100, operation_id="op-t",
                                 trusted=True)
        p2 = self.control.permits("/p", "ses_c")
        self.assertIn("project-paused", p2["blocking"])
        self.assertIn("user-turn-active", p2["blocking"])
        # 暂停态 + 过期 CAS 恢复 → revision-changed（屏障不受影响）
        with self.assertRaises(ActionError) as cm:
            self.use.resume(project="/p", session_id="ses_c", operation_id="op-r",
                            reason="r", expected_revision=0)
        self.assertEqual(cm.exception.detail["refused"], "revision-changed")
        self.assertIsNotNone(self.control.get_session("ses_c")["barrier"],
                             "被拒的恢复不得解除用户回合屏障")
        # 正确 CAS 恢复：只解除项目暂停，屏障仍保留
        out = self.use.resume(project="/p", session_id="ses_c", operation_id="op-r",
                              reason="r", expected_revision=1)
        self.assertTrue(out["executed"] and out["result"]["applied"])
        p3 = self.control.permits("/p", "ses_c")
        self.assertNotIn("project-paused", p3["blocking"])
        self.assertIn("user-turn-active", p3["blocking"],
                      "resume 只改项目维度，不清用户回合屏障")

    # ---- 暂停不改任务状态/ACK/写权 -------------------------------------
    def test_pause_does_not_touch_task_notification_or_gate(self):
        self._claim("/p", "ses_c")
        task = self.ledger.create_task(project="/p", title="t")
        notif = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c", dedupe_key="real:n1",
            payload={"summary": "x"})
        self.use.pause(project="/p", session_id="ses_c", operation_id="op-1",
                       reason="r", expected_revision=0)
        self.assertEqual(self.ledger.get_task(task["id"])["status"], "draft",
                         "项目暂停不得改 Task 状态/取消")
        self.assertEqual(self.ledger.get_notification(notif["id"])["state"],
                         "pending", "项目暂停不得伪造 ACK/投递")

if __name__ == "__main__":
    unittest.main(verbosity=2)
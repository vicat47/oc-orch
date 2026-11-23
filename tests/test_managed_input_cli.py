#!/usr/bin/env python3
"""S4-D CLI：`input` 根命令的语法/守卫/C 视图渲染与回执边界。

被测对象只到 entrypoints（参数/边界/JSON/human 输出）；业务与全部 I/O 均来自
C 组合根用例，测试用 FakeManagedInputs 注入，绝无真实网络、不读生产台账。

守卫顺序：缺确认/缺 owner/缺 reason/缺 expected-revision/非法 intent/缺 park
责任必须在任何业务/Client 调用之前以稳定 usage（退出码 2）拒绝。argparse 必填
参数缺失以 SystemExit(2) 表达；函数级守卫输出稳定 usage 错误到 stderr。
"""

from __future__ import annotations

from contextlib import contextmanager, redirect_stderr, redirect_stdout
from io import StringIO
import inspect
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


class _FakeManagedInputs:
    """C 组合根用例的离线替身：记录调用并按约定返回四态视图。

    四类状态均为字符串（admission/delivery/execution/business_ack）；list 不含
    正文（测试故意塞入 text 字段以验证入口不泄露），get 可看正文。
    """

    def __init__(self):
        self.calls = []

    def _record(self, method, **kw):
        self.calls.append((method, kw))

    def send(self, session_id, **kw):
        self._record("send", session_id=session_id, **kw)
        inp = {
            "input_id": "I-fake-1", "session_id": session_id,
            "message_id": "msg_ocorchF001",
            "intent": kw["intent"], "resume": kw["intent"] == "execute",
            "owner": kw["owner"], "revision": 1,
            "admission": "prepared", "delivery": "pending",
            "execution": "unknown", "business_ack": "unacknowledged",
            "pending_age_ms": 0, "next_action": "none",
        }
        if kw.get("expires_at") is not None:
            inp["expires_at"] = kw["expires_at"]
        if kw["intent"] == "record":
            inp["admission"] = "recorded"
            inp["resume"] = False
        return {"ok": True, "input": inp}

    def send_tracked(self, session_id, **kw):
        self._record("send_tracked", session_id=session_id, **kw)
        inp = {
            "input_id": "I-tracked-1", "session_id": session_id,
            "message_id": kw.get("message_id") or "msg_tracked1",
            "intent": "execute", "resume": True,
            "owner": kw.get("owner") or "ops", "revision": 1,
            "admission": "prepared", "delivery": "pending",
            "execution": "unknown", "business_ack": "unacknowledged",
            "pending_age_ms": 0, "next_action": "observe",
            "tracking": ("untracked" if kw.get("untracked") else "tracked"),
        }
        if kw.get("untracked"):
            return {"ok": True, "tracking": "untracked",
                    "boundary": "explicit-untracked: 纯通信不授予写权",
                    "input": inp,
                    "receipt": {**inp, "boundary": "explicit-untracked: 纯通信不授予写权"}}
        return {"ok": True, "tracking": "tracked", "input": inp,
                "receipt": {**inp, "responsibility_id": "R-tracked",
                            "target_session": kw.get("target_session"),
                            "task_id": kw.get("task_id"),
                            "attempt_id": kw.get("attempt_id")},
                "responsibility": {"id": "R-tracked", "state": "waiting_input"}}

    def get(self, input_id):
        self._record("get", input_id=input_id)
        return {"ok": True, "input": {
            "input_id": input_id, "session_id": "ses_fake",
            "message_id": "msg_ocorchF001",
            "intent": "execute", "resume": True, "owner": "ops", "revision": 1,
            "admission": "admitted", "delivery": "pending",
            "execution": "unknown", "business_ack": "unacknowledged",
            "pending_age_ms": 9000, "next_action": "observe",
            "tracking": "tracked", "task_id": "T-fake",
            "attempt_id": "A-fake", "generation": 2,
            "result_version": "rv-1", "target_session": "ses_coord",
            "responsibility_id": "R-fake",
            "text": "请核对目标状态，勿以空 inbox 判定成功",
        }}

    def list_inputs(self, **kw):
        self._record("list_inputs", **kw)
        return {"ok": True, "inputs": [
            {"input_id": "I-1", "session_id": "ses_a", "message_id": "msg_a",
             "intent": "execute", "resume": True, "owner": "ops", "revision": 2,
             "admission": "delivered", "delivery": "delivered",
             "execution": "succeeded", "business_ack": "acknowledged",
             "pending_age_ms": 0, "next_action": "close",
             "text": "不应出现在列表输出中的正文"},
            {"input_id": "I-2", "session_id": "ses_b", "message_id": "msg_b",
             "intent": "park", "resume": False, "owner": "ops", "revision": 1,
             "admission": "prepared", "delivery": "cancelled",
             "execution": "not-applicable", "business_ack": "not-applicable",
             "pending_age_ms": 3661000, "next_action": "alert"},
        ]}

    def reconcile(self, input_id):
        self._record("reconcile", input_id=input_id)
        return {"ok": True, "input": {
            "input_id": input_id, "admission": "delivered",
            "delivery": "cancelled", "execution": "not-applicable",
            "business_ack": "not-applicable",
        }}

    def acknowledge(self, input_id, **kw):
        self._record("acknowledge", input_id=input_id, **kw)
        return {"ok": True, "changed": bool(kw.get("value")),
                "input": {"input_id": input_id,
                          "business_ack": ("acknowledged" if kw.get("value")
                                           else "unacknowledged")}}

    def flush(self, input_id, **kw):
        self._record("flush", input_id=input_id, **kw)
        return {"ok": True, "input": {"input_id": input_id,
                                      "admission": "cancelled"}}

    def cancel(self, input_id, **kw):
        self._record("cancel", input_id=input_id, **kw)
        return {"ok": True, "input": {"input_id": input_id,
                                      "admission": "cancelled"}}

    def resume_target(self, session_id, **kw):
        self._record("resume_target", session_id=session_id, **kw)
        return {"ok": True, "session_id": session_id, "changed": True,
                "reason": "restored-provider",
                "gate": {"session_id": session_id, "state": "blocked",
                         "source": "provider", "blocked_until": 1777000000000,
                         "category": "rate_limit", "failure_count": 2,
                         "probe_count": 1, "max_half_open_probes": 3,
                         "exhausted": False}}

    def register_watch(self, session_id, **kw):
        self._record("register_watch", session_id=session_id, **kw)
        return {"ok": True, "watch": {
            "session_id": session_id, "owner": kw["owner"],
            "project": kw["project"], "max_pending_age_ms": kw["max_pending_age_ms"],
            "revision": 1,
        }}

    def unregister_watch(self, session_id, **kw):
        self._record("unregister_watch", session_id=session_id, **kw)
        return {"ok": True, "removed": True}

    def list_watches(self, **kw):
        self._record("list_watches", **kw)
        return {"ok": True, "watches": [
            {"session_id": "ses_w", "owner": "ops", "project": "p",
             "max_pending_age_ms": 3600000, "enabled": True, "revision": 3,
             "pending_count": 1, "next_action": "alert",
             "last_observed_at": 1777000000000,
             "last_observation": {
                 "session_id": "ses_w", "owner": "ops", "project": "p",
                 "trusted": True, "pending": 1, "oldest_age_ms": 120000,
                 "aged_out": False, "blocking_reason": None,
                 "next_action": "等待：继续定点观察"}},
            {"session_id": "ses_w2", "owner": "ops", "project": "p2",
             "max_pending_age_ms": 60000, "enabled": False, "revision": 4,
             "last_observation": None},
        ]}

    def observe_registered(self, **kw):
        self._record("observe_registered", **kw)
        return {"ok": True, "observed": [
            {"session_id": "ses_w", "owner": "ops", "project": "p",
             "trusted": True, "pending": 2, "oldest_age_ms": 120000,
             "blocking_reason": None, "aged_out": False,
             "next_action": "等待：继续定点观察"},
        ], "alerts": [], "expiry": {"checked": 1, "alerts": 0, "marked": 0,
                                    "expired": []}}


class ManagedInputCliTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="managed-cli-", dir="/tmp/opencode")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        self.fake = _FakeManagedInputs()
        self.opener = self._opener(self.fake)

    @staticmethod
    def _opener(fake):
        @contextmanager
        def opened(home, *, client_factory=None):
            yield fake
        return opened

    def _parse(self, argv):
        return cli.build_parser().parse_args(["--home", self.tmp] + argv)

    def _exec(self, argv, as_json=False, home=None):
        raw = (["--json"] if as_json else []) + ["--home", home or self.tmp]
        raw += argv
        args = cli.build_parser().parse_args(raw)
        out_buf, err_buf = StringIO(), StringIO()
        with patch.object(cli, "_open_managed_inputs", self.opener), \
                redirect_stdout(out_buf), redirect_stderr(err_buf):
            code = args.func(args)
        return code, out_buf.getvalue(), err_buf.getvalue()

    # ---- 解析与守卫 -------------------------------------------------
    def test_parser_has_input_root_and_required_subcommands(self):
        args = self._parse(["input", "list"])
        self.assertEqual(args.command, "input")
        self.assertEqual(args.input_action, "list")

    def test_send_illegal_intent_is_usage_via_argparse(self):
        with self.assertRaises(SystemExit) as cm:
            self._parse(["input", "send", "--session", "s", "--owner", "o",
                         "--request-id", "r", "--text", "t", "--intent", "bad"])
        self.assertEqual(cm.exception.code, 2)

    def test_send_requires_text_or_file(self):
        with self.assertRaises(SystemExit) as cm:
            self._parse(["input", "send", "--session", "s", "--owner", "o",
                         "--request-id", "r", "--intent", "execute"])
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(self.fake.calls, [], "缺正文不得触碰业务")

    def test_send_park_missing_responsibility_is_usage_before_service(self):
        code, out, err = self._exec(["input", "send", "--session", "s",
                                     "--owner", "o", "--request-id", "r",
                                     "--text", "t", "--intent", "park"])
        self.assertEqual(code, 2)
        self.assertIn("--expires-at", err)
        self.assertIn("--flush-condition", err)
        self.assertEqual(self.fake.calls, [], "缺 park 责任不得触碰业务")

    # ---- 发送 -------------------------------------------------------
    def test_send_execute_passes_all_fields(self):
        code, out, err = self._exec(["input", "send", "--session", "ses_a",
                                     "--intent", "execute", "--owner", "ops",
                                     "--request-id", "r1", "--text", "hello",
                                     "--project", "p"])
        self.assertEqual(code, 0, err)
        self.assertEqual(self.fake.calls, [("send", {
            "session_id": "ses_a", "intent": "execute", "text": "hello",
            "owner": "ops", "request_id": "r1", "project": "p"})])
        self.assertIn("accepted", out)
        self.assertIn("受理≠投递≠执行≠业务ACK", out)

    def test_send_file_reads_text(self):
        path = os.path.join(self.tmp, "prompt.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("from file")
        code, out, err = self._exec(["input", "send", "--session", "s",
                                     "--intent", "execute", "--owner", "o",
                                     "--request-id", "r", "--file", path])
        self.assertEqual(code, 0, err)
        self.assertEqual(self.fake.calls[0][1]["text"], "from file")

    def test_send_tracking_params_align_top_level_send(self):
        code, out, err = self._exec([
            "input", "send", "--session", "ses_w", "--intent", "execute",
            "--owner", "coord", "--request-id", "r-track", "--text", "go",
            "--task", "T-1", "--attempt", "A-1",
            "--continuation-target", "ses_coord", "--deadline", "1777000000123",
            "--material", "baseline-current", "--work-kind", "read_only"])
        self.assertEqual(code, 0, err)
        kw = self.fake.calls[0][1]
        self.assertEqual(kw["task_id"], "T-1")
        self.assertEqual(kw["attempt_id"], "A-1")
        self.assertEqual(kw["target_session"], "ses_coord")
        self.assertEqual(kw["deadline"], 1777000000123)
        self.assertEqual(kw["materials"], ["baseline-current"])
        self.assertEqual(kw["work_kind"], "read_only")
        self.assertNotIn("untracked", kw,
                         "只透传显式参数，不复制默认工作性质/跟踪判断")

    def test_send_park_passes_expires_at_ms_and_flush_condition(self):
        code, out, err = self._exec(["input", "send", "--session", "ses_p",
                                     "--intent", "park", "--owner", "ops",
                                     "--request-id", "r2", "--text", "hold",
                                     "--expires-at", "1777000000123",
                                     "--flush-condition", "coordinator-idle"])
        self.assertEqual(code, 0, err)
        self.assertEqual(self.fake.calls[0][1]["expires_at"], 1777000000123)
        self.assertEqual(self.fake.calls[0][1]["flush_condition"],
                         "coordinator-idle")
        self.assertEqual(self.fake.calls[0][1]["intent"], "park")

    def test_send_record_does_not_claim_delivery(self):
        code, out, err = self._exec(["input", "send", "--session", "s",
                                     "--intent", "record", "--owner", "o",
                                     "--request-id", "r", "--text", "留档"])
        self.assertEqual(code, 0, err)
        self.assertEqual(self.fake.calls[0][1]["intent"], "record")
        self.assertIn("recorded", out)
        self.assertIn("无发送阶段", out)
        self.assertNotIn("delivered", out.lower())

    # ---- 列表/展示 --------------------------------------------------
    def test_list_human_does_not_leak_body_and_shows_four_status(self):
        code, out, err = self._exec(["input", "list"])
        self.assertEqual(code, 0, err)
        self.assertNotIn("不应出现在列表输出中的正文", out)
        self.assertIn("I-1", out)
        self.assertIn("execution=succeeded", out)
        self.assertIn("business_ack=acknowledged", out)
        self.assertIn("列表不含正文", out)

    def test_list_json_does_not_leak_body(self):
        code, out, err = self._exec(["input", "list", "--session", "ses_a"],
                                    as_json=True)
        self.assertEqual(code, 0, err)
        self.assertNotIn("不应出现在列表输出中的正文", out)
        data = json.loads(out)
        self.assertTrue(data["ok"])
        self.assertEqual(len(data["inputs"]), 2)

    def test_list_passes_session_and_limit(self):
        self._exec(["input", "list", "--session", "ses_a", "--limit", "7"])
        self.assertEqual(self.fake.calls, [("list_inputs",
                                            {"session_id": "ses_a", "limit": 7})])

    def test_show_includes_body_and_four_status(self):
        code, out, err = self._exec(["input", "show", "I-x"])
        self.assertEqual(code, 0, err)
        self.assertEqual(self.fake.calls, [("get", {"input_id": "I-x"})])
        self.assertIn("请核对目标状态", out)
        self.assertIn("delivery=pending", out)
        self.assertIn("business_ack=unacknowledged", out)
        self.assertIn("不证明已投递", out)

    def test_reconcile_passes_id(self):
        code, out, err = self._exec(["input", "reconcile", "I-r"])
        self.assertEqual(code, 0, err)
        self.assertEqual(self.fake.calls, [("reconcile", {"input_id": "I-r"})])

    def test_show_prints_top_level_tracking_fields(self):
        code, out, err = self._exec(["input", "show", "I-x"])
        self.assertEqual(code, 0, err)
        self.assertIn("tracking=tracked", out)
        self.assertIn("task=T-fake", out)
        self.assertIn("attempt=A-fake", out)
        self.assertIn("responsibility=R-fake", out)
        self.assertNotIn("tracking={", out, "tracking 是字符串，不是 dict")

    def test_ack_requires_explicit_bool_and_real_confirmation(self):
        code, out, err = self._exec(["input", "ack", "I-1", "--value", "true",
                                     "--owner", "ops", "--confirm", "I-2",
                                     "--expected-revision", "1", "--reason", "ok"])
        self.assertEqual(code, 2)
        self.assertIn("--confirm", err)
        self.assertEqual(self.fake.calls, [])
        with self.assertRaises(SystemExit) as cm:
            self._parse(["input", "ack", "I-1", "--owner", "o",
                         "--confirm", "I-1", "--expected-revision", "1",
                         "--reason", "r"])
        self.assertEqual(cm.exception.code, 2, "缺 --value 必须拒绝")

    def test_ack_true_and_false_are_distinct(self):
        code, out, err = self._exec(["input", "ack", "I-1", "--value", "true",
                                     "--owner", "ops", "--confirm", "I-1",
                                     "--expected-revision", "2", "--reason", "ok"])
        self.assertEqual(code, 0, err)
        self.assertIs(self.fake.calls[0][1]["value"], True)
        self.assertEqual(self.fake.calls[0][1]["expected_revision"], 2)
        self.assertIn("不改变通知收件状态", out)
        self.fake.calls.clear()
        code, out, err = self._exec(["input", "ack", "I-1", "--value", "false",
                                     "--owner", "ops", "--confirm", "I-1",
                                     "--expected-revision", "2", "--reason", "no"])
        self.assertEqual(code, 0, err)
        self.assertIs(self.fake.calls[0][1]["value"], False)
        self.assertIn("value=false 不构成 ACK", out)

    # ---- flush / cancel ---------------------------------------------
    def test_flush_confirm_mismatch_is_usage_before_service(self):
        code, out, err = self._exec(["input", "flush", "I-1", "--owner", "o",
                                     "--confirm", "I-2", "--expected-revision", "1",
                                     "--reason", "cleanup"])
        self.assertEqual(code, 2)
        self.assertIn("--confirm", err)
        self.assertEqual(self.fake.calls, [])

    def test_flush_missing_fields_is_usage(self):
        with self.assertRaises(SystemExit) as cm:
            self._parse(["input", "flush", "I-1", "--owner", "o",
                         "--confirm", "I-1"])
        self.assertEqual(cm.exception.code, 2)
        with self.assertRaises(SystemExit) as cm:
            self._parse(["input", "flush", "I-1", "--confirm", "I-1",
                         "--expected-revision", "1", "--reason", "r"])
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(self.fake.calls, [], "缺 owner 不得触碰业务")

    def test_flush_empty_reason_is_usage(self):
        code, out, err = self._exec(["input", "flush", "I-1", "--owner", "ops",
                                     "--confirm", "I-1", "--expected-revision", "1",
                                     "--reason", ""])
        self.assertEqual(code, 2)
        self.assertIn("--reason", err)
        self.assertEqual(self.fake.calls, [])

    def test_flush_passes_owner_confirm_revision_reason(self):
        code, out, err = self._exec(["input", "flush", "I-1", "--owner", "ops",
                                     "--confirm", "I-1", "--expected-revision", "2",
                                     "--reason", "cleanup done"])
        self.assertEqual(code, 0, err)
        self.assertEqual(self.fake.calls, [("flush", {
            "input_id": "I-1", "owner": "ops", "confirm": "I-1",
            "expected_revision": 2, "reason": "cleanup done"})])
        self.assertIn("flush requested", out)
        self.assertIn("受理≠投递≠执行≠业务ACK", out)

    def test_cancel_passes_same_and_notes_no_implicit_ack(self):
        code, out, err = self._exec(["input", "cancel", "I-1", "--owner", "ops",
                                     "--confirm", "I-1", "--expected-revision", "1",
                                     "--reason", "replaced"])
        self.assertEqual(code, 0, err)
        self.assertEqual(self.fake.calls[0][0], "cancel")
        self.assertIn("cancel requested", out)

    # ---- resume-target ----------------------------------------------
    def test_resume_target_requires_confirm(self):
        code, out, err = self._exec(["input", "resume-target", "--session", "ses_a",
                                     "--owner", "o", "--confirm", "other",
                                     "--expected-revision", "1", "--reason", "r"])
        self.assertEqual(code, 2)
        self.assertIn("--confirm", err)
        self.assertEqual(self.fake.calls, [])

    def test_resume_target_requires_expected_revision_and_reason(self):
        with self.assertRaises(SystemExit) as cm:
            self._parse(["input", "resume-target", "--session", "ses_a",
                         "--owner", "o", "--confirm", "ses_a", "--reason", "r"])
        self.assertEqual(cm.exception.code, 2)
        with self.assertRaises(SystemExit) as cm:
            self._parse(["input", "resume-target", "--session", "ses_a",
                         "--owner", "o", "--confirm", "ses_a",
                         "--expected-revision", "1"])
        self.assertEqual(cm.exception.code, 2)
        self.assertEqual(self.fake.calls, [])

    def test_resume_target_opened_only_lifts_manual_control(self):
        code, out, err = self._exec(["input", "resume-target", "--session", "ses_a",
                                     "--owner", "ops", "--confirm", "ses_a",
                                     "--expected-revision", "3", "--reason", "人工恢复"])
        self.assertEqual(code, 0, err)
        self.assertEqual(self.fake.calls, [("resume_target", {
            "session_id": "ses_a", "owner": "ops", "confirm": "ses_a",
            "expected_revision": 3, "reason": "人工恢复"})])
        self.assertIn("人工控制恢复请求已处理", out)
        self.assertIn("gate.state=blocked", out)
        self.assertIn("exhausted=no", out)
        self.assertIn("probe_budget=1/3", out)
        self.assertIn("不立即触发 prompt", out)
        self.assertNotIn("已执行", out)
        self.assertNotIn("已可自动发送", out)

    # ---- watch 白名单 ------------------------------------------------
    def test_watch_register_converts_seconds_to_ms(self):
        code, out, err = self._exec(["input", "watch", "register",
                                     "--session", "ses_w", "--owner", "ops",
                                     "--project", "p", "--max-pending-age", "90"])
        self.assertEqual(code, 0, err)
        self.assertEqual(self.fake.calls, [("register_watch", {
            "session_id": "ses_w", "owner": "ops", "project": "p",
            "max_pending_age_ms": 90000})])
        self.assertIn("watch registered", out)
        self.assertIn("不自动接管", out)

    def test_watch_register_negative_age_is_usage(self):
        code, out, err = self._exec(["input", "watch", "register",
                                     "--session", "s", "--owner", "o",
                                     "--project", "p", "--max-pending-age", "-1"])
        self.assertEqual(code, 2)
        self.assertIn("--max-pending-age", err)
        self.assertEqual(self.fake.calls, [])

    def test_watch_unregister_requires_confirm(self):
        code, out, err = self._exec(["input", "watch", "unregister",
                                     "--session", "ses_w", "--owner", "o",
                                     "--confirm", "other", "--expected-revision", "1"])
        self.assertEqual(code, 2)
        self.assertIn("--confirm", err)
        self.assertEqual(self.fake.calls, [])

    def test_watch_unregister_passes(self):
        code, out, err = self._exec(["input", "watch", "unregister",
                                     "--session", "ses_w", "--owner", "ops",
                                     "--confirm", "ses_w", "--expected-revision", "3"])
        self.assertEqual(code, 0, err)
        self.assertEqual(self.fake.calls, [("unregister_watch", {
            "session_id": "ses_w", "owner": "ops", "confirm": "ses_w",
            "expected_revision": 3})])

    def test_watch_list_and_observe_render(self):
        code, out, err = self._exec(["input", "watch", "list"])
        self.assertEqual(code, 0, err)
        self.assertIn("ses_w", out)
        code, out, err = self._exec(["input", "watch", "observe", "--limit", "5"])
        self.assertEqual(code, 0, err)
        self.assertEqual(self.fake.calls, [
            ("list_watches", {"limit": 100}),
            ("observe_registered", {"limit": 5}),
        ])

    def test_watch_list_shows_enabled_disabled_and_revision(self):
        code, out, err = self._exec(["input", "watch", "list"])
        self.assertEqual(code, 0, err)
        self.assertIn("state=enabled", out)
        self.assertIn("state=disabled", out)
        self.assertIn("revision=3", out)
        self.assertIn("revision=4", out)

    def test_watch_list_shows_persistent_last_observation(self):
        code, out, err = self._exec(["input", "watch", "list"])
        self.assertEqual(code, 0, err)
        self.assertIn("obs.pending=1", out)
        self.assertIn("obs.oldest_age=2m0s", out)
        self.assertIn("obs.trusted=trusted", out)
        self.assertIn("obs.next=等待：继续定点观察", out)
        self.assertIn("last_observed_at=1777000000000", out)
        # 无持久观察的登记行明确 unknown/未观察，而非伪装已观察
        self.assertIn("observation=unknown(未观察)", out)

    def test_watch_list_read_only_no_client_and_no_observe(self):
        self._exec(["input", "watch", "list"])
        self.assertEqual([c[0] for c in self.fake.calls], ["list_watches"])

    def test_watch_observe_uses_observed_shape_and_shows_responsibility(self):
        code, out, err = self._exec(["input", "watch", "observe"])
        self.assertEqual(code, 0, err)
        self.assertNotIn("(no observations)", out)
        self.assertIn("observe ses_w", out)
        self.assertIn("owner=ops", out)
        self.assertIn("trusted=trusted", out)
        self.assertIn("pending=2", out)
        self.assertIn("oldest_age=2m0s", out)
        self.assertIn("aged_out=no", out)
        self.assertIn("next=等待：继续定点观察", out)

    def test_watch_observe_aged_out_and_blocking_rendered(self):
        self.fake.observe_registered = lambda **kw: {"ok": True, "observed": [
            {"session_id": "ses_x", "owner": "ops2", "trusted": True,
             "pending": 3, "oldest_age_ms": 7200000,
             "blocking_reason": "foreign-input:msg1", "aged_out": True,
             "next_action": "阻塞/超阈值：转 owner 告警，人工决定处置"}],
            "alerts": [], "expiry": {}}
        code, out, err = self._exec(["input", "watch", "observe"])
        self.assertEqual(code, 0, err)
        self.assertIn("observe ses_x", out)
        self.assertIn("owner=ops2", out)
        self.assertIn("aged_out=yes", out)
        self.assertIn("blocking=foreign-input:msg1", out)
        self.assertIn("next=阻塞/超阈值", out)

    def test_watch_observe_empty_only_when_truly_empty(self):
        self.fake.observe_registered = lambda **kw: {
            "ok": True, "observed": [], "alerts": [],
            "expiry": {"checked": 0, "alerts": 0, "marked": 0, "expired": []}}
        code, out, err = self._exec(["input", "watch", "observe"])
        self.assertEqual(code, 0, err)
        self.assertIn("(no observations)", out)

    def test_watch_observe_json_is_unchanged_shape(self):
        code, out, err = self._exec(["input", "watch", "observe"], as_json=True)
        self.assertEqual(code, 0, err)
        data = json.loads(out)
        self.assertTrue(data["ok"])
        self.assertIn("observed", data)
        self.assertIn("alerts", data)
        self.assertIn("expiry", data)
        self.assertEqual(data["observed"][0]["next_action"], "等待：继续定点观察")

    # ---- responsibility 显式共享用例入口 ------------------------------
    def _exec_resp(self, argv, as_json=False, task_status="accepted"):
        calls = []

        class FakeStore:
            class base:
                @staticmethod
                def get_task(task_id):
                    return {"id": task_id, "status": task_status,
                            "closed_at": None}

            def list_open_responsibilities(self, **kw):
                calls.append(("list", kw))
                return [{"id": "R-1", "state": "waiting_input", "task_id": "T-1",
                         "owner": "coord", "target_session": "ses_coord",
                         "wait_kind": "input_execution",
                         "wait_condition": "input:msg:execution",
                         "next_action": "等待执行结束", "next_check_at": 10,
                         "deadline": 20, "budget_used": 0, "max_budget": 3,
                         "revision": 2}]

            def get_responsibility(self, rid):
                calls.append(("get", rid))
                return {"id": rid, "state": "awaiting_review",
                        "task_id": "T-1", "owner": "coord",
                        "wait_kind": "review", "wait_condition": "business",
                        "next_action": "核收", "evidence": {"k": "v"},
                        "next_check_at": 10, "deadline": 20,
                        "budget_used": 1, "max_budget": 3, "revision": 3}

            def transfer_responsibility(self, rid, **kw):
                calls.append(("transfer", rid, kw))
                return {"previous": {"id": rid, "state": "handed_over"},
                        "successor": {"id": "R-2", "owner": kw["successor"]["owner"],
                                      "revision": 1}}

            def resume_responsibility(self, rid, **kw):
                calls.append(("resume", rid, kw))
                return {"id": rid, "state": "handling",
                        "revision": kw["expected_revision"] + 1}

            def update_responsibility(self, rid, **kw):
                calls.append(("update", rid, kw))
                return {"id": rid, "state": kw["fields"]["state"],
                        "wait_condition": kw["fields"]["wait_condition"],
                        "owner": "coord", "revision": kw["expected_revision"] + 1}

            def close_responsibility(self, rid, **kw):
                calls.append(("close", rid, kw))
                return {"id": rid, "state": "done",
                        "close_reason": kw["reason"]}

        @contextmanager
        def opened(home):
            yield FakeStore()

        raw = (["--json"] if as_json else []) + ["--home", self.tmp] + argv
        args = cli.build_parser().parse_args(raw)
        out_buf, err_buf = StringIO(), StringIO()
        with patch.object(cli, "_open_responsibilities", opened), \
                redirect_stdout(out_buf), redirect_stderr(err_buf):
            code = args.func(args)
        return code, out_buf.getvalue(), err_buf.getvalue(), calls

    def test_responsibility_list_and_show(self):
        code, out, err, calls = self._exec_resp(
            ["responsibility", "list", "--task", "T-1", "--owner", "coord"])
        self.assertEqual(code, 0, err)
        self.assertEqual(calls[0][0], "list")
        self.assertIn("R-1", out)
        self.assertIn("state=waiting_input", out)
        self.assertIn("ACK 不等于责任完成", out)
        code, out, err, calls = self._exec_resp(
            ["responsibility", "show", "R-1"])
        self.assertEqual(code, 0, err)
        self.assertEqual(calls[0], ("get", "R-1"))
        self.assertIn("awaiting_review", out)

    def test_responsibility_transfer_wait_close_use_cas(self):
        code, out, err, calls = self._exec_resp(
            ["responsibility", "transfer", "R-1", "--owner", "reviewer",
             "--expected-revision", "3", "--target", "ses_coord",
             "--reason", "handoff"])
        self.assertEqual(code, 0, err)
        self.assertEqual(calls[0][0], "transfer")
        self.assertEqual(calls[0][2]["expected_revision"], 3)
        self.assertEqual(calls[0][2]["successor"]["owner"], "reviewer")
        self.assertEqual(calls[0][2]["successor"]["target_session"], "ses_coord")

        code, out, err, calls = self._exec_resp(
            ["responsibility", "wait", "R-1", "--expected-revision", "3",
             "--state", "waiting_human", "--wait-condition", "operator-decision"])
        self.assertEqual(code, 0, err)
        self.assertEqual(calls[0][2]["expected_revision"], 3)
        self.assertEqual(calls[0][2]["fields"]["state"], "waiting_human")

        code, out, err, calls = self._exec_resp(
            ["responsibility", "close", "R-1", "--expected-revision", "4",
             "--reason", "manual-close"])
        self.assertEqual(code, 0, err)
        close_calls = [c for c in calls if c[0] == "close"]
        self.assertEqual(close_calls[0][2]["reason"], "manual-close")
        self.assertIn("ACK、succeeded、空 inbox 不构成依据", out)

    def test_responsibility_resume_requires_confirm_and_cas(self):
        code, out, err, calls = self._exec_resp(
            ["responsibility", "resume", "R-1", "--confirm", "R-2",
             "--expected-revision", "3", "--reason", "operator"])
        self.assertEqual(code, 2)
        self.assertIn("--confirm", err)
        self.assertEqual(calls, [])
        code, out, err, calls = self._exec_resp(
            ["responsibility", "resume", "R-1", "--confirm", "R-1",
             "--expected-revision", "3", "--reason", "operator"])
        self.assertEqual(code, 0, err)
        self.assertEqual(calls[0][0], "resume")
        self.assertEqual(calls[0][2]["expected_revision"], 3)
        self.assertEqual(calls[0][2]["confirm"], "R-1")
        self.assertIn("不授予写权", out)
        self.assertIn("不解除 task 暂停/取消或目标闸门", out)

    def test_transfer_release_manual_confirm_is_precise_passthrough(self):
        code, out, err, calls = self._exec_resp(
            ["responsibility", "transfer", "R-1", "--owner", "reviewer",
             "--expected-revision", "2", "--release-manual-confirm", "R-1"])
        self.assertEqual(code, 0, err)
        self.assertEqual(calls[0][2]["release_manual_confirmation"], "R-1")

    def test_close_with_superseded_by_goes_through_store_guard(self):
        code, out, err, calls = self._exec_resp(
            ["responsibility", "close", "R-1", "--expected-revision", "3",
             "--reason", "stale-successor", "--superseded-by", "R-2"],
            task_status="result_received")
        self.assertEqual(code, 0, err)
        close_calls = [c for c in calls if c[0] == "close"]
        self.assertEqual(close_calls[0][2]["superseded_by"], "R-2",
                         "非终态合法 handoff 交由 store 事实守卫，不一律 CLI 拒绝")

    def test_responsibility_help_is_usable(self):
        buf = StringIO()
        with redirect_stdout(buf), self.assertRaises(SystemExit) as cm:
            cli.build_parser().parse_args(["responsibility", "resume", "--help"])
        self.assertEqual(cm.exception.code, 0)
        self.assertIn("--confirm", buf.getvalue())
        self.assertIn("--expected-revision", buf.getvalue())

    def test_responsibility_close_requires_reason_and_revision(self):
        with self.assertRaises(SystemExit) as cm:
            self._parse(["responsibility", "close", "R-1",
                         "--expected-revision", "1"])
        self.assertEqual(cm.exception.code, 2)

    def test_responsibility_close_refuses_non_terminal_text_close(self):
        code, out, err, calls = self._exec_resp(
            ["responsibility", "close", "R-1", "--expected-revision", "3",
             "--reason", "manual-close"], task_status="result_received")
        self.assertEqual(code, 2)
        self.assertIn("非终态任务", err)
        self.assertIn("task cancel", err)
        self.assertIn("responsibility wait", err)
        self.assertEqual([c for c in calls if c[0] == "close"], [],
                         "拒绝路径不得调用 close_responsibility")


    def test_legacy_cmd_send_defaults_to_send_tracked(self):
        code, out, err = self._exec(["send", "--session", "ses_w",
                                     "--text", "continue", "--task", "T-1",
                                     "--owner", "coord", "--continuation-target",
                                     "ses_coord", "--deadline", "1777000000123",
                                     "--material", "baseline-current"])
        self.assertEqual(code, 0, err)
        method, kw = self.fake.calls[0]
        self.assertEqual(method, "send_tracked")
        self.assertEqual(kw["text"], "continue")
        self.assertEqual(kw["task_id"], "T-1")
        self.assertEqual(kw["owner"], "coord")
        self.assertEqual(kw["target_session"], "ses_coord")
        self.assertEqual(kw["deadline"], 1777000000123)
        self.assertEqual(kw["materials"], ["baseline-current"])
        self.assertFalse(kw["untracked"])
        self.assertIsNone(kw["work_kind"],
                          "CLI 默认必须传 None，由服务用例拒绝封存目标或要求显式观察")
        self.assertIn("tracking=tracked", out)
        self.assertIn("R-tracked", out)
        self.assertIn("受理≠投递≠执行≠业务ACK", out)
        self.assertIn("接续待办", out)

    def test_send_explicit_untracked_shows_boundary_and_no_write_permission(self):
        code, out, err = self._exec(["send", "--session", "ses_x",
                                     "--text", "hello", "--untracked"])
        self.assertEqual(code, 0, err)
        self.assertTrue(self.fake.calls[0][1]["untracked"])
        self.assertIn("tracking=untracked", out)
        self.assertIn("不授予任务写权", out)
        self.assertIn("未托管边界", out)

    def test_send_json_preserves_legacy_keys_and_adds_tracking(self):
        code, out, err = self._exec(["send", "--session", "ses_w",
                                     "--text", "continue"], as_json=True)
        self.assertEqual(code, 0, err)
        data = json.loads(out)
        self.assertTrue(data["ok"])
        self.assertIn("message_id", data)
        self.assertIn("message", data)
        self.assertEqual(data["tracking"], "tracked")
        self.assertEqual(data["input"]["input_id"], "I-tracked-1")
        self.assertEqual(data["responsibility"]["id"], "R-tracked")

    def test_legacy_send_parser_semantics_preserved(self):
        args = self._parse(["send", "--session", "s", "--text", "t",
                            "--delivery", "steer"])
        self.assertEqual(args.command, "send")
        self.assertEqual(args.session, "s")
        self.assertEqual(args.text, "t")
        self.assertEqual(args.file, None)
        self.assertEqual(args.message_id, None)
        self.assertEqual(args.delivery, "steer")
        self.assertIsNone(args.work_kind,
                          "CLI 不得把缺省工作性质猜成 observe（封存目标须拒绝）")
        self.assertFalse(args.untracked)

    def test_work_kind_accepts_explicit_read_only(self):
        args = self._parse(["send", "--session", "s", "--text", "t",
                            "--work-kind", "read_only"])
        self.assertEqual(args.work_kind, "read_only")


if __name__ == "__main__":
    unittest.main()
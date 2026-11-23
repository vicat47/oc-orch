#!/usr/bin/env python3
"""AgentlyMailTransport 契约测试（基于已核实的 +send/+search 输出字段）。

只注入 subprocess 桩 runner，绝不触碰真实邮箱/凭据；断言：
- +send 参数构造（--confirmed/--to/--subject/--body/--body-format plain）；
- **发件身份 fail-closed**：未注入 SenderBinding、或 ref/identity/verified 不匹配
  → MailAuthError，绝不把任意策略 identity 当真实发送账号（+send 命令本身忽略
  sender 参数，只有绑定的已核实身份可信）；
- 只有 ``queued=true`` 才报告受理；``confirmation_required`` 中间态 →
  MailAuthError；非零退出/超时/无法解析 → MailUnknownError（可能已提交，绝不
  只凭非零退出重发）；可执行缺失 → MailChannelError；本地坏请求 → MailRequestError
  （不调用 runner）。
- +search 只消费已核实字段（message_id/from/to/created_at/subject/snippet/
  is_read），其余丢弃；零条返回 []；非零/超时/ok!=true → MailSearchUnavailable。
覆盖 M04/M10 的传输侧契约。参考 evidence/2026-11-14-coordinator-probe-recovery/
contracts.json（agently-cli 1.0.18，只读核验，未发送/未登录）。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from oc_orch.adapters.agently_mail import (  # noqa: E402
    DEFAULT_SEARCH_COMMAND,
    DEFAULT_SEND_COMMAND,
    AgentlyMailTransport,
    RunnerResult,
    SenderBinding,
)
from oc_orch.application.mail_port import (  # noqa: E402
    MailAuthError,
    MailChannelError,
    MailRequestError,
    MailSearchUnavailableError,
    MailUnknownError,
)

SENDER_REF = "oc-ops"
SENDER_IDENTITY = "ops@example.test"


class StubRunner:
    """按预设 stdout/stderr/returncode 返回的 runner 桩；记录调用。"""

    def __init__(self, *, result: RunnerResult | None = None,
                 exc: Exception | None = None):
        self.result = result
        self.exc = exc
        self.calls: list[list[str]] = []

    def __call__(self, argv, timeout_seconds):
        self.calls.append((list(argv), timeout_seconds))
        if self.exc is not None:
            raise self.exc
        return self.result or RunnerResult(returncode=0, stdout="", stderr="")


def transport(runner, *, binding: SenderBinding | None = None):
    return AgentlyMailTransport(send_command=list(DEFAULT_SEND_COMMAND),
                                search_command=list(DEFAULT_SEARCH_COMMAND),
                                runner=runner,
                                sender_binding=binding)


GOOD_BINDING = SenderBinding(ref=SENDER_REF, identity=SENDER_IDENTITY,
                             verified=True, version="1.0.18")


def _send(t, **kw):
    base = dict(to="operator@example.test", sender_ref=SENDER_REF,
                sender=SENDER_IDENTITY, subject="主题", body="正文",
                timeout_seconds=5.0)
    base.update(kw)
    return t.send(**base)


class TestSenderBindingFailClosed(unittest.TestCase):
    def test_no_binding_refuses_send(self):
        stub = StubRunner(result=RunnerResult(0, json.dumps({"queued": True})))
        t = transport(stub, binding=None)
        with self.assertRaises(MailAuthError):
            _send(t)
        self.assertEqual(stub.calls, [])  # 未注入绑定 → 不调用外部 CLI

    def test_ref_mismatch_refuses(self):
        stub = StubRunner()
        t = transport(stub, binding=GOOD_BINDING)
        with self.assertRaises(MailAuthError):
            _send(t, sender_ref="other-ref")

    def test_identity_mismatch_refuses(self):
        stub = StubRunner()
        t = transport(stub, binding=GOOD_BINDING)
        with self.assertRaises(MailAuthError):
            _send(t, sender="stranger@example.net")

    def test_unverified_binding_refuses(self):
        stub = StubRunner()
        t = transport(stub, binding=SenderBinding(ref=SENDER_REF,
                                                  identity=SENDER_IDENTITY,
                                                  verified=False))
        with self.assertRaises(MailAuthError):
            _send(t)

    def test_no_login_or_install_surface(self):
        t = transport(StubRunner(), binding=GOOD_BINDING)
        self.assertFalse(hasattr(t, "login"))
        self.assertFalse(hasattr(t, "install"))
        binding = GOOD_BINDING
        self.assertEqual(binding.version, "1.0.18")  # 锁定版本供审计/预检


class TestSendContract(unittest.TestCase):
    def _make_with_binding(self, stdout: str, returncode: int = 0,
                           stderr: str = "", exc: Exception | None = None):
        stub = StubRunner(result=RunnerResult(returncode, stdout, stderr),
                          exc=exc)
        return transport(stub, binding=GOOD_BINDING), stub

    def test_success_queued_true_is_reported(self):
        t, stub = self._make_with_binding(json.dumps({"queued": True, "ok": True}))
        out = _send(t)
        self.assertEqual(out, {"queued": True})
        argv, timeout = stub.calls[0]
        self.assertIn("--confirmed", argv)
        self.assertIn("--to", argv)
        self.assertIn("operator@example.test", argv)
        self.assertIn("--subject", argv)
        self.assertIn("--body", argv)
        self.assertIn("--body-format", argv)
        self.assertIn("plain", argv)
        self.assertEqual(timeout, 5.0)
        # 命令本身不携带 sender 参数：身份仅由绑定审计（fail-closed 的由来）
        self.assertNotIn("--from", argv)

    def test_confirmation_required_is_auth_error(self):
        t, _ = self._make_with_binding(json.dumps(
            {"confirmation_required": True, "confirmation_token": "tok"}))
        with self.assertRaises(MailAuthError):
            _send(t)

    def test_nonzero_exit_is_unknown_not_resend(self):
        # 非零退出不能证明未提交 → MailUnknownError（绝不立即重发）。
        t, _ = self._make_with_binding("", returncode=1, stderr="boom")
        with self.assertRaises(MailUnknownError):
            _send(t)

    def test_timeout_is_unknown(self):
        t, _ = self._make_with_binding("", exc=subprocess.TimeoutExpired("cmd", 5.0))
        with self.assertRaises(MailUnknownError):
            _send(t)

    def test_missing_executable_is_channel_error(self):
        t, _ = self._make_with_binding("", exc=FileNotFoundError("agently-cli"))
        with self.assertRaises(MailChannelError):
            _send(t)

    def test_exit0_without_queued_is_unknown(self):
        for stdout in ("", "{}", json.dumps({"ok": True}), "not-json"):
            with self.subTest(stdout=stdout):
                t, _ = self._make_with_binding(stdout)
                with self.assertRaises(MailUnknownError):
                    _send(t)

    def test_local_invalid_request_no_runner_call(self):
        stub = StubRunner()
        t = transport(stub, binding=GOOD_BINDING)
        with self.assertRaises(MailRequestError):
            _send(t, to="")
        with self.assertRaises(MailRequestError):
            _send(t, subject="")
        with self.assertRaises(MailRequestError):
            _send(t, body="")
        with self.assertRaises(MailRequestError):
            _send(t, body="x" * (1_000_001))
        self.assertEqual(stub.calls, [])


class TestSearchContract(unittest.TestCase):
    SEARCH_HIT = {
        "message_id": "msg_9zqx0", "created_at": "2026-11-14T19:52:44+08:00",
        "from": {"name": "Ops", "email": "ops@example.test"},
        "to": [{"name": "Operator", "email": "operator@example.test"}],
        "subject": "主题", "snippet": "<highlight>主题</highlight>",
        "is_read": False, "unknown_extra": {"a": 1},
    }

    def _ok_response(self, hits):
        return json.dumps({"ok": True, "data": {"data": hits,
                                                "pagination": {"has_more": False}}})

    def test_search_only_keeps_verified_fields(self):
        stub = StubRunner(result=RunnerResult(0, self._ok_response(
            [dict(self.SEARCH_HIT)])))
        t = transport(stub, binding=GOOD_BINDING)
        out = t.search_receipt(subject="主题", to="operator@example.test",
                               after_ms=1_000_000, before_ms=2_000_000,
                               limit=50, timeout_seconds=5.0)
        self.assertEqual(len(out), 1)
        hit = out[0]
        self.assertEqual(hit["message_id"], "msg_9zqx0")
        self.assertEqual(hit["from"]["email"], "ops@example.test")
        self.assertEqual(hit["to"][0]["email"], "operator@example.test")
        self.assertEqual(hit["subject"], "主题")
        self.assertEqual(hit["snippet"], "<highlight>主题</highlight>")
        self.assertNotIn("unknown_extra", hit)
        argv, _ = stub.calls[0]
        self.assertIn("--q", argv)
        self.assertIn("--search-in", argv)
        self.assertIn("SEARCH_IN_SUBJECT", argv)
        self.assertIn("--to", argv)
        self.assertIn("--after", argv)
        self.assertIn("--before", argv)
        self.assertIn("--limit", argv)
        self.assertIn("--dir", argv)
        self.assertIn("sent", argv)  # 只查发件箱，不把收件箱同主题当自身回执

    def test_search_empty_list_is_matchless_not_error(self):
        stub = StubRunner(result=RunnerResult(0, self._ok_response([])))
        t = transport(stub, binding=GOOD_BINDING)
        self.assertEqual(t.search_receipt(subject="s", to="a@b.test",
                                          after_ms=1, before_ms=2,
                                           limit=10, timeout_seconds=5.0), [])

    def test_search_truncated_or_missing_coverage_cannot_prove_unique_receipt(self):
        for pagination in (None, {}, {"has_more": True}, {"has_more": "false"}):
            with self.subTest(pagination=pagination):
                payload = {"ok": True, "data": {"data": [dict(self.SEARCH_HIT)]}}
                if pagination is not None:
                    payload["data"]["pagination"] = pagination
                stub = StubRunner(result=RunnerResult(0, json.dumps(payload)))
                t = transport(stub, binding=GOOD_BINDING)
                with self.assertRaises(MailSearchUnavailableError):
                    t.search_receipt(subject="主题", to="operator@example.test",
                                     after_ms=1, before_ms=2,
                                     limit=50, timeout_seconds=5.0)
                self.assertEqual(len(stub.calls), 1)

    def test_search_nonzero_is_unavailable(self):
        t = transport(StubRunner(result=RunnerResult(1, "", "err")),
                      binding=GOOD_BINDING)
        with self.assertRaises(MailSearchUnavailableError):
            t.search_receipt(subject="s", to="a@b.test", after_ms=1,
                             before_ms=2, limit=10, timeout_seconds=5.0)

    def test_search_timeout_or_missing_exec_is_unavailable(self):
        for exc in (subprocess.TimeoutExpired("cmd", 5.0),
                    FileNotFoundError("agently-cli")):
            with self.subTest(exc=exc):
                t = transport(StubRunner(exc=exc), binding=GOOD_BINDING)
                with self.assertRaises(MailSearchUnavailableError):
                    t.search_receipt(subject="s", to="a@b.test", after_ms=1,
                                     before_ms=2, limit=10, timeout_seconds=5.0)

    def test_search_ok_false_or_bad_json_is_unavailable(self):
        for stdout in (json.dumps({"ok": False, "data": {"data": []}}), "nope"):
            with self.subTest(stdout=stdout):
                t = transport(StubRunner(result=RunnerResult(0, stdout)),
                              binding=GOOD_BINDING)
                with self.assertRaises(MailSearchUnavailableError):
                    t.search_receipt(subject="s", to="a@b.test", after_ms=1,
                                     before_ms=2, limit=10, timeout_seconds=5.0)


class TestSearchBindingGate(unittest.TestCase):
    def test_search_without_binding_is_unavailable_zero_cli(self):
        stub = StubRunner(result=RunnerResult(0, json.dumps(
            {"ok": True, "data": {"data": []}})))
        t = transport(stub, binding=None)
        with self.assertRaises(MailSearchUnavailableError):
            t.search_receipt(subject="s", to="a@b.test", after_ms=1,
                             before_ms=2, limit=10, timeout_seconds=5.0)
        self.assertEqual(stub.calls, [])  # 0 外部 CLI：读取真实账号前已拒绝

    def test_search_unverified_binding_is_unavailable(self):
        stub = StubRunner()
        t = transport(stub, binding=SenderBinding(ref=SENDER_REF,
                                                  identity=SENDER_IDENTITY,
                                                  verified=False))
        with self.assertRaises(MailSearchUnavailableError):
            t.search_receipt(subject="s", to="a@b.test", after_ms=1,
                             before_ms=2, limit=10, timeout_seconds=5.0)
        self.assertEqual(stub.calls, [])


class TestNestedConfirmation(unittest.TestCase):
    def test_nested_data_confirmation_required_is_auth_error(self):
        # contracts.json 原文：confirmation 字段实际在 data.confirmation_required。
        stub = StubRunner(result=RunnerResult(0, json.dumps({
            "data": {"confirmation_required": True,
                     "summary": {"action": "send"}}})))
        t = transport(stub, binding=GOOD_BINDING)
        with self.assertRaises(MailAuthError):
            _send(t)
        # 需要先调用一次 CLI 才能从输出识别交互确认态；绝不以交互 token 续发
        self.assertEqual(len(stub.calls), 1)

    def test_top_level_confirmation_still_recognized(self):
        stub = StubRunner(result=RunnerResult(0, json.dumps(
            {"confirmation_required": True})))
        t = transport(stub, binding=GOOD_BINDING)
        with self.assertRaises(MailAuthError):
            _send(t)
        self.assertEqual(len(stub.calls), 1)  # 调用一次才可知交互确认态


class TestContractConstants(unittest.TestCase):
    def test_commands_match_recorded_contract(self):
        # contracts.json 记录的 CLI 位置：agently-cli message +send / +search
        self.assertEqual(DEFAULT_SEND_COMMAND[:3], ["agently-cli", "message", "+send"])
        self.assertEqual(DEFAULT_SEARCH_COMMAND[:3],
                         ["agently-cli", "message", "+search"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

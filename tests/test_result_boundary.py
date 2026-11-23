#!/usr/bin/env python3
"""验收兼容门面的边界回归：委托方向、窄 I/O 注入与 mock.patch 拦截。

本文件不镜像 F2 门禁用例（见 test_verification_atomicity），只固封“重构边界”：
- oc_orch.results 必须是指向 adapters.results 的兼容别名（单一权威）；
- prepare/verify 的业务判定位于 application.verification，窄 I/O
  （read_result/run_checks_io/clock）由兼容门面注入，门面不重实现业务；
- 兼容门面必须保持旧入口调用签名，并让 mock.patch 旧入口可拦截编排；
- domain.rules.SITE_HOLDING 与全部尝试状态对齐（未显式释放均持有写权）。
"""

from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oc_orch.adapters import results as adapter_results  # noqa: E402
from oc_orch.application import verification as usecase  # noqa: E402
from oc_orch.db import init  # noqa: E402
from oc_orch.domain import rules  # noqa: E402
from oc_orch.ledger import Ledger  # noqa: E402
from oc_orch.results import (  # noqa: E402
    prepare_verification,
    verify_attempt,
)
from oc_orch.util import now_ms  # noqa: E402


class _FakeClient:
    """只提供 collect_attempt 所需的 messages()，绝不触网。"""

    def __init__(self, messages):
        self._messages = messages

    def messages(self, session_id, limit=None, order=None):
        return self._messages


class _Snippet:
    @staticmethod
    def build(text="任务完成"):
        return {"type": "assistant", "time": {"created": now_ms()},
                "content": [{"type": "text", "text": text}]}


def _user_anchor(msg_id: str, at: int):
    return {"id": msg_id, "type": "user", "text": "go", "time": {"created": at}}


class FacadeWiringTests(unittest.TestCase):
    """兼容门面：单一权威 + 旧入口签名 + patch 拦截。"""

    def test_results_facade_identity(self):
        self.assertIs(
            importlib.import_module("oc_orch.results"),
            importlib.import_module("oc_orch.adapters.results"),
        )
        self.assertEqual(prepare_verification, adapter_results.prepare_verification)
        self.assertEqual(verify_attempt, adapter_results.verify_attempt)

    def test_prepare_delegates_to_application_with_injected_io(self):
        """旧入口 prepare 委托 application.prepare_verification，窄 I/O 已注入。"""
        ledger_sentinel = object()
        seen = {}

        def fake_prepare(attempt_id, **kw):
            seen.update(kw)
            return {"prepared": {}}

        with mock.patch.object(usecase, "prepare_verification",
                               side_effect=fake_prepare) as m:
            prepare_verification(ledger_sentinel, "A1", result="pass")
        m.assert_called_once_with(
            "A1", result="pass", reason="", evidence=(), notes="", force=False,
            run_checks=False, check_timeout=120, actor="coordinator",
            ledger=ledger_sentinel, read_result=unittest.mock.ANY,
            run_checks_io=unittest.mock.ANY, clock=unittest.mock.ANY,
            changed_check=unittest.mock.ANY,
        )
        self.assertTrue(callable(seen["read_result"]))
        self.assertTrue(callable(seen["run_checks_io"]))
        self.assertTrue(callable(seen["clock"]))

    def test_verify_honors_patch_of_legacy_prepare(self):
        """mock.patch oc_orch.results.prepare_verification 必须拦截兼容入口编排。

        夹具走真实合法闭环（新协议）：派单前封存缺失基线 → 产物出现（new）→
        record_terminal 真实成功区间 → collect 门面 → 真实 prepare → patched verify。
        """
        home = tempfile.mkdtemp(prefix="oc-orch-boundary-")
        conn = init(home)
        self.addCleanup(conn.close)
        ledger = Ledger(conn, home)
        out_file = os.path.join(home, "out.txt")
        t = ledger.create_task(project="p", title="b")
        a = ledger.create_attempt(t["id"], directory=home, status="dispatched",
                                  dispatch_state="confirmed",
                                  session_id="ses-boundary",
                                  deliverables=["out.txt"],
                                  dispatch_msg_id="msg-boundary")
        # 派单前基线：out.txt 尚不存在 → 本轮出现即来源 new
        adapter_results.scan_deliverable_baseline(ledger, a["id"])
        with open(out_file, "w", encoding="utf-8") as f:
            f.write("done")
        from tests.fakes import FakeClient
        client = FakeClient()
        t0 = 1_000_000
        client.append_message("ses-boundary", _user_anchor("msg-boundary", t0))
        client.append_message("ses-boundary", {
            "id": "msg-boundary-a0", "type": "assistant",
            "time": {"created": t0 + 10},
            "content": [{"type": "text", "text": "任务完成"}]})
        # 合法证据入口提交成功终态（可证明输入→结束区间）
        ledger.record_terminal(
            a["id"], outcome="succeeded", source="rest",
            event_id="evt-boundary",
            candidate={"outcome": "succeeded", "source": "rest",
                       "ended_at": t0 + 100, "session_id": "ses-boundary",
                       "interval": {"proven": True, "input_at": t0,
                                    "via": "boundary-fixture"}})
        from oc_orch.results import collect_attempt

        collect_attempt(ledger, client, a["id"],
                        {"result_max_messages": 50, "result_store_limit": 20000},
                        auto=False)
        # 用真实 prepare 产出合法载荷作为“被 patch 的返回”，证明拦截发生且行为不变。
        real = prepare_verification
        sentinel = real(ledger, a["id"], result="pass", evidence=["e"])
        with mock.patch("oc_orch.results.prepare_verification",
                        return_value=sentinel) as m:
            out = verify_attempt(ledger, a["id"], result="pass", evidence=["e"])
        m.assert_called_once()
        self.assertTrue(out["changed"])
        self.assertEqual(ledger.get_attempt(a["id"])["status"], "accepted")
        self.assertEqual(ledger.get_task(t["id"])["status"], "accepted")


class ApplicationVerificationPortsTests(unittest.TestCase):
    """application.verification 只通过注入端口交互，不依赖 adapter/util。"""

    def test_prepare_uses_injected_ports_only(self):
        class FakeLedger:
            def __init__(self, snap):
                self.snap = snap
                self.snapshot_calls = 0

            def verification_snapshot(self, attempt_id):
                self.snapshot_calls += 1
                return dict(self.snap)

            def get_attempt(self, attempt_id):  # pragma: no cover
                raise AssertionError("prepare 不应调用 get_attempt")

            def submit_verification(self, *a, **k):  # pragma: no cover
                raise AssertionError("prepare 不应调用 submit_verification")

        task = {"id": "T1", "current_attempt_id": "A1", "status": "result_received",
                "closed_at": None}
        att = {"id": "A1", "task_id": "T1", "generation": 1,
               "status": "terminal_succeeded", "metadata": {},
               "result_collected_at": None, "result_path": None,
               "result_summary": None}
        snap = {"attempt_id": "A1", "task_id": "T1", "fingerprint": "fp",
                "snapshot_at": 5, "attempt": att, "task": task,
                "deliverables": []}
        run_log = []
        real_clock = [7]
        outcome = usecase.prepare_verification(
            "A1", result="fail", reason="需要返工", run_checks=True,
            check_timeout=30, ledger=FakeLedger(snap),
            read_result=lambda att_, task_: (None, None),
            run_checks_io=lambda aid, to: run_log.append((aid, to)),
            clock=lambda: real_clock[0],
        )
        self.assertEqual(run_log, [("A1", 30)])
        self.assertEqual(outcome["prepared"]["verified_at"], 7)
        self.assertEqual(outcome["prepared"]["result"], "fail")
        self.assertEqual(outcome["prepared"]["attempt_id"], "A1")


class RulesInvariantTests(unittest.TestCase):
    """主 agent 已确认的写权现场修复：SITE_HOLDING 对齐全部尝试状态。"""

    def test_site_holding_covers_every_attempt_status(self):
        self.assertEqual(set(rules.SITE_HOLDING), set(rules.ATTEMPT_STATUSES))


if __name__ == "__main__":
    unittest.main()
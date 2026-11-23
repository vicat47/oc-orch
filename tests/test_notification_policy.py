#!/usr/bin/env python3
"""S1 通知有效性纯规则单元测试（wake-delivery-control Requirement 1）。

只测试 `oc_orch.domain.notification_policy` 的纯规则契约：
- `notification_decision(notification, *, task, attempt)` 的 action/reason 稳定码；
- kind 类别注册（current_only / cleanup / coordinator / unknown）；
- obsolete generation、已解除条件、人工暂停/取消/accepted/closed、
  cleanup 责任与缺归属/未知的诊断 defer 行为；
- 已登记 kind 两 ID（task_id/attempt_id）皆空 → legacy-unscoped send
  （无任何 attempt 锚，不推断不存在的 generation）；
- 条件/状态的“已知”必须由 domain.rules 状态枚举验证，未知不得猜解除；
- 用 AST 静态提取实际 callsite 的 literal kind，防止产生端新增 kind 漏登记；
- 不修改输入、不写 ACK/结果版本（纯规则无副作用）。

不依赖真实 Service/Client；按项目约定在文件头注入 src 到 sys.path。
"""

from __future__ import annotations

import ast
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "src"))

from oc_orch.domain.notification_policy import (  # noqa: E402
    ALL_KINDS,
    CATEGORY_CLEANUP,
    CATEGORY_COORDINATOR,
    CATEGORY_CURRENT_ONLY,
    CATEGORY_UNKNOWN,
    CLEANUP_KINDS,
    COORDINATOR_KINDS,
    CURRENT_ONLY_KINDS,
    kind_category,
    notification_decision,
)

# runtime/supervisor.py、application/collection.py 及 entrypoints/cli.py 中
# 实际产出的通知 kind 全集（手工盘点；含 classify_publication 等动态构造，
# AST 无法自动追踪的动态 kind 由此表覆盖，见 test_ast_* 的覆盖限度说明）。
ACTUAL_PRODUCER_KINDS = [
    # supervisor.py —— 当前进展/状态（attempt 归属）
    "result_ready", "result_missing", "result_collect_failed",
    "execution_succeeded", "execution_failed", "execution_interrupted",
    "attempt_lost", "stall", "idle_unconfirmed", "provider_retry",
    "provider_blocked", "blocked_permission", "blocked_form", "dispatch_unknown",
    # supervisor.py —— 旧执行清理责任（attempt 归属）
    "side_effects_running", "execution_unconfirmed",
    # supervisor.py + entrypoints/cli.py —— 按 session/租约路由，无 attempt 归属
    "coordinator_recovery", "coordinator_blocked_permission",
    "coordinator_manual_recovery",
    # supervisor.py —— S4 执行后接续（任务级后续责任；任务结束/暂停仍受守卫）
    "continuation_ready", "responsibility_missing",
]

# AST 提取的 producer 源文件（相对仓库根）。
PRODUCER_SOURCES = [
    "src/oc_orch/runtime/supervisor.py",
    "src/oc_orch/application/collection.py",
    "src/oc_orch/entrypoints/cli.py",
]
_ENQUEUE_FUNCS = {"enqueue_task", "enqueue_notification"}


def _extract_kind_literals(tree: ast.AST) -> set[str]:
    """从 AST 提取 callsite 的字面 kind（enqueue_task/enqueue_notification）。"""
    kinds: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        name = None
        if isinstance(fn, ast.Name):
            name = fn.id
        elif isinstance(fn, ast.Attribute):
            name = fn.attr
        if name not in _ENQUEUE_FUNCS:
            continue
        # 关键字 kind=".."
        for kw in node.keywords:
            if kw.arg == "kind" and isinstance(kw.value, ast.Constant) \
                    and isinstance(kw.value.value, str):
                kinds.add(kw.value.value)
        # enqueue_task(self, task, kind, ...)：kind 为位置参数第 2 项（index 1）
        if name == "enqueue_task" and len(node.args) >= 2:
            v = node.args[1]
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                kinds.add(v.value)
    return kinds


def _notification(kind, *, task_id=None, attempt_id=None, payload=None,
                  state="pending"):
    return {
        "id": "N-test",
        "kind": kind,
        "task_id": task_id,
        "attempt_id": attempt_id,
        "payload": dict(payload or {}),
        "state": state,
        "dedupe_key": f"{kind}:test",
        "project": "p",
        "target_session": "ses_coord",
        "created_at": 1000,
        "next_attempt_at": 1000,
        "priority": 5,
        "max_attempts": 3,
        "metadata": {},
        "attempts": 0,
        "sent_at": None,
        "acked_at": None,
        "message_id": None,
    }


def _task(status="running", *, current_attempt_id="A-cur", closed_at=None,
          accepted_at=None):
    return {
        "id": "T-1",
        "status": status,
        "current_attempt_id": current_attempt_id,
        "closed_at": closed_at,
        "accepted_at": accepted_at,
        "metadata": {},
        "project": "p",
    }


def _attempt(status="running", *, attempt_id="A-cur", task_id="T-1",
             site_released_at=None, stop_requested_at=None,
             stop_confirmed_at=None, metadata=None):
    return {
        "id": attempt_id,
        "task_id": task_id,
        "generation": 1,
        "status": status,
        "site_released_at": site_released_at,
        "stop_requested_at": stop_requested_at,
        "stop_confirmed_at": stop_confirmed_at,
        "terminal_at": None,
        "terminal_kind": None,
        "metadata": dict(metadata or {}),
    }


class KindRegistryTests(unittest.TestCase):
    """任务 1.1：所有实际通知 kind 都登记规则，不允许遗漏为 unknown。"""

    def test_all_producer_kinds_are_registered(self):
        for kind in ACTUAL_PRODUCER_KINDS:
            with self.subTest(kind=kind):
                self.assertIn(kind, ALL_KINDS)
                self.assertNotEqual(kind_category(kind), "unknown")

    def test_handwritten_inventory_matches_registry(self):
        # 手工盘点表（含动态构造的 result_ready/result_collect_failed 等）
        # 与模块登记表必须一致；防止盘点和登记两侧脱节。
        self.assertEqual(
            set(ACTUAL_PRODUCER_KINDS),
            set(ALL_KINDS),
        )
        self.assertEqual(
            set(ALL_KINDS),
            set(CURRENT_ONLY_KINDS) | set(CLEANUP_KINDS) | set(COORDINATOR_KINDS),
        )

    def test_ast_extracted_producer_literals_are_registered(self):
        # 静态保证：能从 producer 源提取的 callsite 字面 kind 必须已登记。
        # 动态构造（kind 变量 / f-string，如 result_ready、coordinator_<outcome>
        # 告警）无法自动追溯，属明确记录的覆盖限度，由手工盘点表补齐。
        extracted: set[str] = set()
        for rel in PRODUCER_SOURCES:
            path = os.path.join(ROOT, rel)
            with self.subTest(producer=rel):
                with open(path, encoding="utf-8") as f:
                    tree = ast.parse(f.read())
                extracted |= _extract_kind_literals(tree)
        self.assertTrue(extracted, "未能从任何可解析 producer 源提取 kind 文字量")
        missing = sorted(extracted - set(ALL_KINDS))
        self.assertEqual(
            missing, [],
            "以下产生端 callsite 字面 kind 未登记："
            f"{missing}（动态构造的覆盖限度见手工盘点表）",
        )

    def test_each_kind_has_stable_category(self):
        self.assertEqual(kind_category("result_ready"), CATEGORY_CURRENT_ONLY)
        self.assertEqual(kind_category("stall"), CATEGORY_CURRENT_ONLY)
        self.assertEqual(kind_category("idle_unconfirmed"), CATEGORY_CURRENT_ONLY)
        self.assertEqual(kind_category("provider_retry"), CATEGORY_CURRENT_ONLY)
        self.assertEqual(kind_category("provider_blocked"), CATEGORY_CURRENT_ONLY)
        self.assertEqual(kind_category("blocked_permission"), CATEGORY_CURRENT_ONLY)
        self.assertEqual(kind_category("blocked_form"), CATEGORY_CURRENT_ONLY)
        self.assertEqual(kind_category("execution_failed"), CATEGORY_CURRENT_ONLY)
        self.assertEqual(kind_category("execution_interrupted"), CATEGORY_CURRENT_ONLY)
        self.assertEqual(kind_category("attempt_lost"), CATEGORY_CURRENT_ONLY)
        self.assertEqual(kind_category("dispatch_unknown"), CATEGORY_CURRENT_ONLY)
        self.assertEqual(kind_category("side_effects_running"), CATEGORY_CLEANUP)
        self.assertEqual(kind_category("execution_unconfirmed"), CATEGORY_CLEANUP)
        self.assertEqual(kind_category("coordinator_recovery"), CATEGORY_COORDINATOR)
        self.assertEqual(
            kind_category("coordinator_blocked_permission"), CATEGORY_COORDINATOR,
        )
        self.assertEqual(
            kind_category("coordinator_manual_recovery"), CATEGORY_COORDINATOR,
        )
        self.assertNotEqual(
            kind_category("some_future_kind"), CATEGORY_CURRENT_ONLY)
        self.assertEqual(kind_category("some_future_kind"), CATEGORY_UNKNOWN)


class CurrentOnlyTests(unittest.TestCase):
    """当前进展/状态类：必须匹配 current attempt 且任务未结束才 send。"""

    def test_current_and_open_sends(self):
        task = _task(current_attempt_id="A-cur")
        attempt = _attempt("stalled", attempt_id="A-cur")
        out = notification_decision(
            _notification("stall", task_id="T-1", attempt_id="A-cur"),
            task=task, attempt=attempt,
        )
        self.assertEqual(out["action"], "send")
        self.assertEqual(out["reason"], "current-only-active")
        self.assertEqual(out["kind_category"], CATEGORY_CURRENT_ONLY)
        self.assertEqual(out["current_attempt_id"], "A-cur")
        self.assertEqual(out["attempt_id"], "A-cur")

    def test_obsolete_generation_suppressed(self):
        task = _task(current_attempt_id="A-new")
        attempt = _attempt("result_collected", attempt_id="A-old")
        out = notification_decision(
            _notification("result_ready", task_id="T-1", attempt_id="A-old",
                          payload={"result_version": "rv1-abc"}),
            task=task, attempt=attempt,
        )
        self.assertEqual(out["action"], "suppress")
        self.assertEqual(out["reason"], "superseded")
        self.assertEqual(out["kind_category"], CATEGORY_CURRENT_ONLY)

    def test_superseded_status_kinds_never_wake(self):
        # P01 纯规则形态：旧代次 stall/idle_unconfirmed/provider_retry
        # 一律 suppress（由上层记 non-ACK 的 dead/resolved，不烧预算）。
        task = _task(current_attempt_id="A-new")
        old = _attempt("idle_unconfirmed", attempt_id="A-old")
        for kind in ("stall", "idle_unconfirmed", "provider_retry"):
            with self.subTest(kind=kind):
                out = notification_decision(
                    _notification(kind, task_id="T-1", attempt_id="A-old"),
                    task=task, attempt=old,
                )
                self.assertEqual(out["action"], "suppress")
                self.assertEqual(out["reason"], "superseded")

    def test_task_paused_defers(self):
        task = _task("paused", current_attempt_id="A-cur")
        attempt = _attempt("running", attempt_id="A-cur")
        out = notification_decision(
            _notification("stall", task_id="T-1", attempt_id="A-cur"),
            task=task, attempt=attempt,
        )
        self.assertEqual(out["action"], "defer")
        self.assertEqual(out["reason"], "task-paused")

    def test_task_cancelled_suppresses(self):
        task = _task("cancelled", current_attempt_id="A-cur", closed_at=5000)
        attempt = _attempt("running", attempt_id="A-cur")
        out = notification_decision(
            _notification("stall", task_id="T-1", attempt_id="A-cur"),
            task=task, attempt=attempt,
        )
        self.assertEqual(out["action"], "suppress")
        self.assertEqual(out["reason"], "task-cancelled")

    def test_task_accepted_suppresses(self):
        task = _task("accepted", current_attempt_id="A-cur", accepted_at=5000)
        attempt = _attempt("result_collected", attempt_id="A-cur")
        out = notification_decision(
            _notification("result_ready", task_id="T-1", attempt_id="A-cur"),
            task=task, attempt=attempt,
        )
        self.assertEqual(out["action"], "suppress")
        self.assertEqual(out["reason"], "task-accepted")

    def test_closed_task_suppresses_even_if_status_odd(self):
        # closed_at 是任务结束的显式证据：即使 status 未变也抑制。
        task = _task("running", current_attempt_id="A-cur", closed_at=9000)
        attempt = _attempt("running", attempt_id="A-cur")
        out = notification_decision(
            _notification("stall", task_id="T-1", attempt_id="A-cur"),
            task=task, attempt=attempt,
        )
        self.assertEqual(out["action"], "suppress")
        self.assertEqual(out["reason"], "task-closed")

    def test_condition_kind_resolved_suppressed(self):
        task = _task(current_attempt_id="A-cur")
        attempt = _attempt("running", attempt_id="A-cur")
        for kind in ("blocked_permission", "blocked_form", "provider_retry",
                     "stall", "idle_unconfirmed", "dispatch_unknown"):
            with self.subTest(kind=kind):
                out = notification_decision(
                    _notification(kind, task_id="T-1", attempt_id="A-cur"),
                    task=task, attempt=attempt,
                )
                # attempt.status=running 是条件已解除的明确证据（枚举内已知）
                self.assertEqual(out["action"], "suppress")
                self.assertEqual(out["reason"], "condition-resolved")

    def test_condition_kind_still_pending_sends(self):
        task = _task(current_attempt_id="A-cur")
        cases = [
            ("blocked_permission", "blocked_permission"),
            ("blocked_form", "blocked_form"),
            ("provider_retry", "provider_retry"),
            ("provider_blocked", "provider_retry"),
            ("stall", "stalled"),
            ("idle_unconfirmed", "idle_unconfirmed"),
            ("dispatch_unknown", "dispatch_unknown"),
        ]
        for kind, pending_status in cases:
            with self.subTest(kind=kind):
                attempt = _attempt(pending_status, attempt_id="A-cur")
                out = notification_decision(
                    _notification(kind, task_id="T-1", attempt_id="A-cur"),
                    task=task, attempt=attempt,
                )
                self.assertEqual(out["action"], "send")
                self.assertEqual(out["reason"], "current-only-active")

    def test_condition_kind_with_unknown_status_defers(self):
        # “条件解除仅凭明确可信证据”：attempt.status 缺失/未知不得猜 resolved。
        task = _task(current_attempt_id="A-cur")
        for attempt in ({"id": "A-cur", "task_id": "T-1"},  # 无 status
                        _attempt("mystery-status", attempt_id="A-cur")):
            with self.subTest(attempt=attempt):
                out = notification_decision(
                    _notification("blocked_permission", task_id="T-1",
                                  attempt_id="A-cur"),
                    task=task, attempt=attempt,
                )
                self.assertEqual(out["action"], "defer")
                self.assertEqual(out["reason"], "status-unknown")

    def test_task_status_unknown_defers(self):
        # 任务 status 不在 TASK_STATUSES 枚举内：不是已知，不能据此判断
        # 人工控制/结束，也不能放行发送。
        task = _task("mystery-status", current_attempt_id="A-cur")
        attempt = _attempt("result_collected", attempt_id="A-cur")
        out = notification_decision(
            _notification("result_ready", task_id="T-1", attempt_id="A-cur"),
            task=task, attempt=attempt,
        )
        self.assertEqual(out["action"], "defer")
        self.assertEqual(out["reason"], "task-status-unknown")

    def test_missing_current_pointer_defers(self):
        # task.current_attempt_id=None：不能视为已匹配/未取代任何已知 attempt。
        task = {"id": "T-1", "status": "running", "current_attempt_id": None,
                "closed_at": None}
        attempt = _attempt("stalled", attempt_id="A-cur")
        out = notification_decision(
            _notification("stall", task_id="T-1", attempt_id="A-cur"),
            task=task, attempt=attempt,
        )
        self.assertEqual(out["action"], "defer")
        self.assertEqual(out["reason"], "current-unresolved")

    def test_result_version_and_inputs_preserved(self):
        payload = {"result_version": "rv1-abc", "summary": "x"}
        notif = _notification("result_ready", task_id="T-1", attempt_id="A-cur",
                              payload=payload)
        task = _task(current_attempt_id="A-cur")
        attempt = _attempt("result_collected", attempt_id="A-cur")
        before = {k: notif[k] for k in ("id", "attempt_id", "state", "acked_at")}
        out = notification_decision(notif, task=task, attempt=attempt)
        # 结果版本与 ACK/身份不变量原样保留（纯规则不触碰台账）。
        self.assertEqual(out["result_version"], "rv1-abc")
        self.assertEqual(notif, {
            **notif, "payload": payload,
        })
        self.assertEqual({k: notif[k] for k in before}, before)

    def test_registered_both_ids_empty_sends_legacy_unscoped(self):
        # 无 task_id/attempt_id 的既有全局通知（NotificationBase/独立通道）：
        # 已登记 kind 两 ID 皆空 → legacy-unscoped send，不推断不存在代次。
        for kind in ("stall", "result_ready", "side_effects_running",
                     "execution_unconfirmed", "coordinator_recovery"):
            with self.subTest(kind=kind):
                out = notification_decision(
                    _notification(kind),
                    task=_task(current_attempt_id="A-other"), attempt=None,
                )
                self.assertEqual(out["action"], "send")
                self.assertEqual(out["reason"], "legacy-unscoped")
                # “不推断不存在的 generation”：即使传入 task.current 指向
                # 其它 attempt，也不能按代次 suppress/取代。
                self.assertNotEqual(out["reason"], "superseded")

    def test_current_only_task_id_only_defers_on_open_task(self):
        # 带 task_id 无 attempt：current-only 无法判 current/generation → defer。
        out = notification_decision(
            _notification("result_ready", task_id="T-1"),
            task=_task(current_attempt_id="A-cur"), attempt=None,
        )
        self.assertEqual(out["action"], "defer")
        self.assertEqual(out["reason"], "attempt-unresolved")

    def test_current_only_task_id_only_honors_manual_control(self):
        # 带 task_id 无 attempt 仍先按人工控制处理：
        # 取消/结束 → suppress；暂停 → defer。
        for status, action in (("cancelled", "suppress"),
                               ("accepted", "suppress"),
                               ("paused", "defer")):
            with self.subTest(status=status):
                task = _task(status, current_attempt_id="A-cur",
                             closed_at=5000 if status == "cancelled" else None,
                             accepted_at=5000 if status == "accepted" else None)
                out = notification_decision(
                    _notification("result_ready", task_id="T-1"),
                    task=task, attempt=None,
                )
                self.assertEqual(out["action"], action)
                self.assertNotEqual(out["reason"], "legacy-unscoped")


class CleanupTests(unittest.TestCase):
    """旧执行清理：副作用/未确认停止/写权持有是可见责任，不随代次/结束消失。"""

    def test_side_effects_running_requires_cleanup(self):
        task = _task(current_attempt_id="A-cur")
        attempt = _attempt("terminal_succeeded", attempt_id="A-cur",
                           site_released_at=None,
                           metadata={"side_effect_checks": {
                               "shells": {"state": "active"},
                               "session": {"state": "inactive"},
                           }})
        out = notification_decision(
            _notification("side_effects_running", task_id="T-1",
                          attempt_id="A-cur"),
            task=task, attempt=attempt,
        )
        self.assertEqual(out["action"], "cleanup_required")
        self.assertEqual(out["reason"], "side-effects-active")
        self.assertEqual(out["kind_category"], CATEGORY_CLEANUP)

    def test_execution_unconfirmed_requires_cleanup(self):
        task = _task(current_attempt_id="A-cur")
        attempt = _attempt("idle_unconfirmed", attempt_id="A-cur",
                           site_released_at=None)
        out = notification_decision(
            _notification("execution_unconfirmed", task_id="T-1",
                          attempt_id="A-cur"),
            task=task, attempt=attempt,
        )
        self.assertEqual(out["action"], "cleanup_required")
        self.assertEqual(out["reason"], "stop-unconfirmed")

    def test_cleanup_not_silenced_by_manual_task_end(self):
        # 人工取消/accepted/paused/closed 不能静默忽略未释放写权/未确认停止。
        for task_status in ("cancelled", "accepted", "paused"):
            with self.subTest(task_status=task_status):
                task = _task(task_status, current_attempt_id="A-cur",
                             closed_at=5000 if task_status == "cancelled" else None)
                attempt = _attempt("terminal_succeeded", attempt_id="A-cur",
                                   site_released_at=None)
                for kind in ("side_effects_running", "execution_unconfirmed"):
                    with self.subTest(kind=kind):
                        out = notification_decision(
                            _notification(kind, task_id="T-1",
                                          attempt_id="A-cur"),
                            task=task, attempt=attempt,
                        )
                        self.assertEqual(out["action"], "cleanup_required")

    def test_cleanup_after_site_released_suppressed(self):
        # 释放且无明确活动矛盾 → 可抑制。
        task = _task(current_attempt_id="A-cur")
        attempt = _attempt("terminal_succeeded", attempt_id="A-cur",
                           site_released_at=6000)
        for kind in ("side_effects_running", "execution_unconfirmed"):
            with self.subTest(kind=kind):
                out = notification_decision(
                    _notification(kind, task_id="T-1", attempt_id="A-cur"),
                    task=task, attempt=attempt,
                )
                self.assertEqual(out["action"], "suppress")
                self.assertEqual(out["reason"], "site-released")

    def test_site_released_but_active_side_effects_keeps_cleanup(self):
        # 释放标记不能掩盖仍明确活动的 shell/子 session 矛盾：
        # 已有明确 active 证据 → 仍 cleanup_required（保留可见诊断，不改写权）。
        cases = [
            {"side_effect_checks": {"shells": {"state": "active"}}},
            {"running_shells": [{"id": "sh1"}], "active_children": []},
            {"active_children": ["ses_child"]},
            {"side_effect_checks": {"session": {"state": "unknown"}}},
        ]
        for meta in cases:
            with self.subTest(meta=meta):
                attempt = _attempt("terminal_succeeded", attempt_id="A-cur",
                                   site_released_at=6000, metadata=meta)
                out = notification_decision(
                    _notification("side_effects_running", task_id="T-1",
                                  attempt_id="A-cur"),
                    task=_task(current_attempt_id="A-cur"), attempt=attempt,
                )
                self.assertEqual(out["action"], "cleanup_required")
                self.assertEqual(out["reason"], "side-effects-active")

    def test_cleanup_overshadowed_by_newer_generation_still_visible(self):
        # 旧代次尝试的副作用/未确认停止不能被“current 已变化”静默丢掉：
        # 旧尝试写权未释放就是事实，转 cleanup_required。
        task = _task(current_attempt_id="A-new")
        old = _attempt("terminal_succeeded", attempt_id="A-old",
                       site_released_at=None)
        out = notification_decision(
            _notification("side_effects_running", task_id="T-1",
                          attempt_id="A-old"),
            task=task, attempt=old,
        )
        self.assertEqual(out["action"], "cleanup_required")

    def test_cleanup_kind_site_held_general_reason(self):
        # 无 active 观察但有写权未释放：仍清理可见（write-site-held）。
        task = _task(current_attempt_id="A-cur")
        attempt = _attempt("terminal_succeeded", attempt_id="A-cur",
                           site_released_at=None)
        out = notification_decision(
            _notification("side_effects_running", task_id="T-1",
                          attempt_id="A-cur"),
            task=task, attempt=attempt,
        )
        self.assertEqual(out["action"], "cleanup_required")
        self.assertEqual(out["reason"], "write-site-held")


class CoordinatorTests(unittest.TestCase):
    """coordinator/基础设施：按目标/租约路由，不套用 attempt 代次过滤。"""

    def test_coordinator_kinds_send_without_attempt_scoping(self):
        for kind in ("coordinator_recovery", "coordinator_blocked_permission",
                     "coordinator_manual_recovery"):
            with self.subTest(kind=kind):
                out = notification_decision(
                    _notification(kind), task=None, attempt=None,
                )
                self.assertEqual(out["action"], "send")
                self.assertEqual(out["reason"], "legacy-unscoped")
                self.assertEqual(out["kind_category"], CATEGORY_COORDINATOR)

    def test_coordinator_kind_with_unprovable_attempt_defers(self):
        # 防御：coordinator 通知若错误携带 attempt_id 且无法证明归属，仍 defer。
        out = notification_decision(
            _notification("coordinator_recovery", attempt_id="A-ghost"),
            task=None, attempt=None,
        )
        self.assertEqual(out["action"], "defer")
        self.assertEqual(out["reason"], "attempt-unattributable")


class AttributionTests(unittest.TestCase):
    """缺归属 / 无法证明归属 → 保留诊断的可诊断 defer。"""

    def test_attempt_id_without_attempt_dict_defers(self):
        out = notification_decision(
            _notification("stall", task_id="T-1", attempt_id="A-cur"),
            task=_task(current_attempt_id="A-cur"), attempt=None,
        )
        self.assertEqual(out["action"], "defer")
        self.assertEqual(out["reason"], "attempt-unattributable")

    def test_attempt_id_mismatch_defers(self):
        out = notification_decision(
            _notification("stall", task_id="T-1", attempt_id="A-cur"),
            task=_task(current_attempt_id="A-cur"),
            attempt=_attempt("stalled", attempt_id="A-other"),
        )
        self.assertEqual(out["action"], "defer")
        self.assertEqual(out["reason"], "attempt-unattributable")

    def test_attempt_task_mismatch_defers(self):
        out = notification_decision(
            _notification("stall", task_id="T-1", attempt_id="A-cur"),
            task=_task(current_attempt_id="A-cur"),
            attempt=_attempt("stalled", attempt_id="A-cur", task_id="T-2"),
        )
        self.assertEqual(out["action"], "defer")
        self.assertEqual(out["reason"], "attempt-task-mismatch")

    def test_task_dict_missing_defers_current_only(self):
        out = notification_decision(
            _notification("stall", task_id="T-1", attempt_id="A-cur"),
            task=None,
            attempt=_attempt("stalled", attempt_id="A-cur"),
        )
        self.assertEqual(out["action"], "defer")
        self.assertEqual(out["reason"], "task-unattributable")

    def test_current_only_with_task_dict_missing_defers(self):
        # task 缺失不能证明 current/已结束 → 不猜、不发送（defer 保留责任）。
        out = notification_decision(
            _notification("result_ready", task_id="T-1", attempt_id="A-cur"),
            task=None,
            attempt=_attempt("result_collected", attempt_id="A-cur"),
        )
        self.assertEqual(out["action"], "defer")
        self.assertEqual(out["reason"], "task-unattributable")

    def test_attempt_provided_without_task_id_still_attributable(self):
        # 通知缺 task_id，但 attempt 与 task 可证明归属：仍能正确判定。
        task = _task(current_attempt_id="A-cur")
        attempt = _attempt("stalled", attempt_id="A-cur")
        out = notification_decision(
            _notification("stall", attempt_id="A-cur"),
            task=task, attempt=attempt,
        )
        self.assertEqual(out["action"], "send")
        self.assertEqual(out["task_id"], "T-1")

    def test_unknown_kind_defers_with_diagnostic(self):
        out = notification_decision(
            _notification("brand_new_kind", task_id="T-1", attempt_id="A-cur"),
            task=_task(current_attempt_id="A-cur"),
            attempt=_attempt("result_collected", attempt_id="A-cur"),
        )
        self.assertEqual(out["action"], "defer")
        self.assertEqual(out["reason"], "unknown-kind")
        self.assertEqual(out["kind_category"], "unknown")

    def test_invalid_notification_defers(self):
        out = notification_decision("not-a-dict", task=None, attempt=None)
        self.assertEqual(out["action"], "defer")
        self.assertEqual(out["reason"], "invalid-notification")


class StabilityTests(unittest.TestCase):
    """决策输出形状与稳定字段契约。"""

    def test_decision_shape_is_stable(self):
        out = notification_decision(
            _notification("result_ready", task_id="T-1", attempt_id="A-cur",
                          payload={"result_version": "rv1-x"}),
            task=_task(current_attempt_id="A-cur"),
            attempt=_attempt("result_collected", attempt_id="A-cur"),
        )
        for key in ("action", "reason", "kind_category", "kind", "task_id",
                    "attempt_id", "current_attempt_id"):
            self.assertIn(key, out)
        self.assertEqual(out["action"], "send")
        self.assertEqual(out["reason"], "current-only-active")
        self.assertEqual(out["attempt_status"], "result_collected")
        self.assertEqual(out["task_status"], "running")

    def test_unscoped_shapes_stable(self):
        out = notification_decision(
            _notification("stall"), task=None, attempt=None)
        self.assertEqual(out["action"], "send")
        self.assertEqual(out["reason"], "legacy-unscoped")
        self.assertIsNone(out["attempt_id"])
        self.assertIsNone(out["task_id"])
        self.assertIsNone(out["current_attempt_id"])

    def test_reasons_are_stable_codes(self):
        # 抽查：关键稳定码不得随实现漂移。
        task = _task(current_attempt_id="A-new")
        out = notification_decision(
            _notification("result_ready", task_id="T-1", attempt_id="A-old"),
            task=task, attempt=_attempt("terminal_succeeded", attempt_id="A-old"),
        )
        self.assertEqual(out["reason"], "superseded")


if __name__ == "__main__":
    unittest.main()

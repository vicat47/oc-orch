#!/usr/bin/env python3
"""S4-A 后续责任台账独立验收（真实临时 SQLite WAL，两连接）。

覆盖不变量（不连真实服务、不读生产台账）：

- 登记：必填/类型/状态校验；任务不存在/已终结拒绝、暂停只允许 waiting_human；
  **事务内身份一致性**（project / attempt 属 task / session / generation /
  result_version / 输入回执 session+message）；已被新代次取代只能记历史
  waiting_human；同一 (task, condition_key, generation, result_version) 幂等。
- 更新：revision CAS；字段白名单（owner 与身份字段不可改写，result_version
  不能洗白）；预算单调、`max_budget` 不得自动提高；`waiting_human` 不能被普通
  update 自动恢复（需显式 release 确认）；暂停/终结/新代次不能推进。
- 转交：单事务“旧责任 handed_over + 后继已登记 + prior/successor 互链”；不得
  跨 task/project、降低 budget_used、提高 max_budget、延长 deadline、改写
  result_version；waiting_human 只能转给 waiting_human（除非显式确认）。
- 关闭：只有任务真实 accepted/cancelled（权威事实）或已验证替代后继
  （superseded_by）才能关闭；manual-close/duplicate/transferred/任意
  superseded:* 文本、ACK、succeeded 一律拒绝；任务不存在 → waiting_human 诊断。
- 发布：必须有可核验待核收事实/缺失诊断；状态规范为 awaiting_review 与通知
  **同事务**；去重键含责任 id+condition_key+条件版本且不含 now；ACK 后不新建；
  无路由/暂停/期限预算耗尽 → waiting_human + 去重告警；新代次 → 拒绝。
- 并发/重启：两连接并发登记归并一行；同 revision 更新只有一个赢家；预算与
  到期时间跨重启保持；发布崩溃（BEFORE INSERT 触发器）整体回滚。
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO))

from oc_orch.adapters.responsibility_ledger import (
    ResponsibilityStore,
    condition_version,
    MISSING_CONDITION_KEY,
    STATE_AWAITING_REVIEW,
    STATE_HANDLING,
    STATE_WAITING_EXECUTION,
    STATE_WAITING_HUMAN,
    STATE_WAITING_INPUT,
)
from oc_orch.adapters.sqlite_db import init
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.db import connect
from oc_orch.domain.errors import LedgerError
from tests.fakes import FakeClock

T0 = 1_800_000_000_000


def _defaults(**over):
    kw = dict(
        owner="coord", project="proj", task_id="T-1", attempt_id="A-1",
        generation=1, result_version=None, input_id=None,
        session_id="ses_worker", message_id="msg_in", target_session="ses_coord",
        state=STATE_AWAITING_REVIEW, wait_kind="worker-report",
        wait_condition="worker 结构化回报到达", next_action="核收回报并判断下一步",
        next_check_at=T0 + 60_000, deadline=T0 + 3_600_000,
        condition_key="await-worker-report", evidence={"source": "test"},
        max_budget=3,
    )
    kw.update(over)
    return kw


def _review_evidence(**over):
    ev = {"source": "test",
          "execution": {"input_at": T0 - 500, "ended_at": T0 - 1,
                        "idle_id": "msg_idle", "idle_outcome": "succeeded",
                        "coverage": "complete"}}
    ev.update(over)
    return ev


class RespBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="oc-orch-resp-", dir="/tmp/opencode")
        self.addCleanup(self.tmp.cleanup)
        self.home = self.tmp.name
        self.clock = FakeClock(wall_ms=T0)
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home, clock=self.clock)
        self.store = ResponsibilityStore(self.ledger)
        self.task = self.ledger.create_task(project="proj", title="t", task_id="T-1",
                                            coordinator_session="ses_coord")
        self.attempt = self.ledger.create_attempt(
            "T-1", attempt_id="A-1", directory=self.home,
            session_id="ses_worker", generation=1, status="running")
        self.addCleanup(self._cleanup_conns)

    def _cleanup_conns(self):
        for conn in getattr(self, "_extra_conns", []):
            try:
                conn.close()
            except Exception:
                pass

    def second(self):
        conn = init(self.home)
        self._extra_conns = getattr(self, "_extra_conns", []) + [conn]
        ledger = Ledger(conn, self.home, clock=self.clock)
        return ledger, ResponsibilityStore(ledger)

    def rid(self, **over):
        return self.store.open_responsibility(**_defaults(**over))["id"]

    def review_rid(self, **over):
        """带真实执行结束证据的待核收责任。"""
        base = {"state": STATE_AWAITING_REVIEW, "evidence": _review_evidence()}
        base.update(over)
        return self.store.open_responsibility(**_defaults(**base))["id"]

    def mark_task_accepted(self, task_id="T-1"):
        """模拟经 submit_verification 后的真实 accepted 结果（测试夹具）。"""
        self.conn.execute(
            "UPDATE tasks SET status='accepted', accepted_at=?, updated_at=? "
            "WHERE id=?", (T0, T0, task_id))

    def new_attempt(self, attempt_id="A-2", generation=2,
                    session_id="ses_worker2"):
        self.ledger.release_site("A-1", confirmed=True, external_stopped=True,
                                 actor="test")
        return self.ledger.create_attempt(
            "T-1", attempt_id=attempt_id, directory=self.home,
            session_id=session_id, generation=generation, status="running",
            expected_current="A-1")

    def notifications(self, state=None):
        return self.ledger.list_notifications(target_session="ses_coord",
                                              state=state, limit=100)

    def notif_count(self, conn=None):
        c = conn or self.conn
        return c.execute("SELECT COUNT(*) FROM notifications").fetchone()[0]

    def alerts(self):
        return self.ledger.list_alerts(state="new", limit=100)


class TestOpen(RespBase):
    def test_open_persists_all_contract_fields(self):
        out = self.store.open_responsibility(**_defaults(
            input_id="I-1", evidence={"k": "v"}))
        self.assertTrue(out["created"])
        for key in ("id", "project", "task_id", "attempt_id", "generation",
                    "result_version", "input_id", "session_id", "message_id",
                    "owner", "target_session", "state", "wait_kind",
                    "wait_condition", "next_action", "next_check_at", "deadline",
                    "condition_key", "revision", "budget_used", "max_budget",
                    "evidence", "prior_id", "successor_id", "created_at",
                    "updated_at", "closed_at", "close_reason"):
            self.assertIn(key, out)
        self.assertEqual(out["revision"], 1)
        self.assertEqual(out["budget_used"], 0)
        self.assertEqual(out["max_budget"], 3)
        self.assertEqual(out["evidence"], {"k": "v"})
        self.assertIsNone(out["closed_at"])
        self.assertEqual(self.store.get_responsibility(out["id"])["state"],
                         STATE_AWAITING_REVIEW)

    def test_open_returns_row_with_id_alias_and_positional_compat(self):
        out = self.store.open_responsibility(**_defaults())
        self.assertEqual(out["responsibility_id"], out["id"])
        self.assertEqual(out["responsibility"]["id"], out["id"])
        self.assertEqual(out["deadline"], self.store.get_responsibility(
            out["id"])["deadline"])
        positional = self.store.open_responsibility(
            "coord", "proj", "T-1", "A-1", 1, None, None, "ses_worker",
            "msg_in", "ses_coord",
            state=STATE_AWAITING_REVIEW, wait_kind="worker-report",
            wait_condition="worker 结构化回报到达",
            next_action="核收回报并判断下一步",
            next_check_at=T0 + 60_000, deadline=T0 + 3_600_000,
            condition_key="await-worker-report")
        self.assertEqual(positional["id"], out["id"])
        self.assertFalse(positional["created"])

    def test_open_is_idempotent_for_same_condition_and_version(self):
        first = self.store.open_responsibility(**_defaults())
        again = self.store.open_responsibility(**_defaults(next_check_at=T0 + 900_000))
        self.assertTrue(first["created"])
        self.assertFalse(again["created"])
        self.assertEqual(first["id"], again["id"])
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM responsibilities").fetchone()[0], 1)
        self.assertEqual(again["next_check_at"], T0 + 60_000,
                         "幂等返回原行，不被后一次入参改写")

    def test_new_generation_or_condition_is_new_identity(self):
        a = self.store.open_responsibility(**_defaults())["id"]
        b = self.store.open_responsibility(
            **_defaults(attempt_id=None, generation=None, condition_key="ck-b"))
        c = self.store.open_responsibility(
            **_defaults(condition_key="ck-c"))["id"]
        self.assertTrue(b["created"])
        self.assertEqual(len({a, c, b["id"]}), 3)

    def test_open_rejects_missing_or_blank_required_fields(self):
        for field in ("owner", "project", "task_id", "wait_kind", "wait_condition",
                      "next_action", "condition_key"):
            for value in ("", "   ", None):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(LedgerError) as ctx:
                        self.store.open_responsibility(**_defaults(**{field: value}))
                    self.assertEqual(ctx.exception.code, "RESPONSIBILITY_INVALID")

    def test_open_rejects_bad_types_and_unknown_state(self):
        for over in ({"state": "invented"}, {"state": "done"},
                     {"generation": -1}, {"generation": True},
                     {"max_budget": -1}, {"evidence": ["not", "dict"]},
                     {"next_check_at": "soon"}, {"deadline": T0 * 1000},
                     {"session_id": ""}):
            with self.subTest(over=over):
                with self.assertRaises(LedgerError) as ctx:
                    self.store.open_responsibility(**_defaults(**over))
                self.assertEqual(ctx.exception.code, "RESPONSIBILITY_INVALID")

    def test_open_requires_project_match(self):
        with self.assertRaises(LedgerError) as ctx:
            self.store.open_responsibility(**_defaults(project="other-project"))
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_PROJECT_MISMATCH")

    def test_open_rejects_attempt_of_another_task(self):
        other = self.ledger.create_task(project="proj", title="unrelated",
                                        task_id="T-2")
        with self.assertRaises(LedgerError) as ctx:
            self.store.open_responsibility(**_defaults(task_id=other["id"]))
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_ATTEMPT_MISMATCH")
        self.assertEqual(
            self.store.list_open_responsibilities(task_id=other["id"]), [])

    def test_open_rejects_session_and_generation_mismatch(self):
        with self.assertRaises(LedgerError) as ctx:
            self.store.open_responsibility(**_defaults(session_id="ses_other"))
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_SESSION_MISMATCH")
        with self.assertRaises(LedgerError) as ctx2:
            self.store.open_responsibility(**_defaults(generation=5))
        self.assertEqual(ctx2.exception.code, "RESPONSIBILITY_GENERATION_MISMATCH")
        with self.assertRaises(LedgerError) as ctx3:
            self.store.open_responsibility(**_defaults(attempt_id=None, generation=5))
        self.assertEqual(ctx3.exception.code, "RESPONSIBILITY_GENERATION_MISMATCH")

    def test_open_rejects_input_receipt_mismatch(self):
        self.conn.execute(
            "INSERT INTO session_inputs(input_id, session_id, message_id, source, "
            "intent, outstanding, revision, admission_state, prepared_at, "
            "created_at, updated_at) VALUES('I-real','ses_worker','msg_real',"
            "'managed_input','execute',0,1,'prepared',?,?,?)", (T0, T0, T0))
        with self.assertRaises(LedgerError) as ctx:
            self.store.open_responsibility(**_defaults(
                input_id="I-real", message_id="msg_wrong"))
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_INPUT_MISMATCH")
        ok = self.store.open_responsibility(**_defaults(
            input_id="I-real", message_id="msg_real"))
        self.assertTrue(ok["created"])
        # 输入行不存在时不强绑（历史/外部），但存在即必须一致
        self.store.open_responsibility(**_defaults(
            input_id="I-absent", message_id="msg_any", condition_key="ck-x"))

    def test_open_rejects_terminal_and_missing_task(self):
        self.ledger.transition_task("T-1", "cancelled", actor="test")
        with self.assertRaises(LedgerError) as ctx:
            self.store.open_responsibility(**_defaults())
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_TASK_TERMINAL")
        with self.assertRaises(LedgerError) as ctx2:
            self.store.open_responsibility(**_defaults(task_id="T-missing"))
        self.assertEqual(ctx2.exception.code, "RESPONSIBILITY_TASK_NOT_FOUND")

    def test_paused_task_allows_only_waiting_human(self):
        self.ledger.transition_task("T-1", "paused", actor="op")
        with self.assertRaises(LedgerError) as ctx:
            self.store.open_responsibility(**_defaults(state=STATE_WAITING_INPUT))
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_TASK_PAUSED")
        ok = self.store.open_responsibility(**_defaults(state=STATE_WAITING_HUMAN))
        self.assertEqual(ok["state"], STATE_WAITING_HUMAN)

    def test_stale_attempt_can_only_be_recorded_as_waiting_human(self):
        self.new_attempt()
        with self.assertRaises(LedgerError) as ctx:
            self.store.open_responsibility(**_defaults(
                condition_key="ck-old", generation=1, attempt_id="A-1"))
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_STALE")
        hist = self.store.open_responsibility(**_defaults(
            condition_key="ck-old", generation=1, attempt_id="A-1",
            state=STATE_WAITING_HUMAN))
        self.assertEqual(hist["state"], STATE_WAITING_HUMAN)

    def test_condition_version_is_stable_and_time_free(self):
        self.assertEqual(condition_version(1, None), "g1.r-")
        self.assertEqual(condition_version(None, "rv1-abc"), "g-.rrv1-abc")
        self.assertEqual(condition_version(3, "rv1-abc"), "g3.rrv1-abc")

    def test_missing_table_raises_without_ddl(self):
        raw = sqlite3.connect(":memory:", isolation_level=None)
        self.addCleanup(raw.close)
        raw.row_factory = sqlite3.Row
        store = ResponsibilityStore(self.ledger, conn=raw)
        with self.assertRaises(LedgerError) as ctx:
            store.list_open_responsibilities()
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_SCHEMA_MISSING")
        self.assertEqual(raw.execute(
            "SELECT COUNT(*) FROM sqlite_master").fetchone()[0], 0,
            "构造/查询不得自动建表")


class TestQuery(RespBase):
    def test_home_uses_wal_journal(self):
        self.assertEqual(self.conn.execute("PRAGMA journal_mode").fetchone()[0].lower(),
                         "wal")

    def test_list_open_filters_orders_and_excludes_closed(self):
        a = self.rid(condition_key="ck-a", next_check_at=T0 + 5_000)
        b = self.rid(condition_key="ck-b", next_check_at=T0 + 1_000, owner="other")
        c = self.rid(condition_key="ck-c", next_check_at=T0 + 2_000)
        rows = self.store.list_open_responsibilities()
        self.assertEqual([r["id"] for r in rows], [b, c, a])
        self.assertEqual(len(self.store.list_open_responsibilities(owner="other")), 1)
        self.assertEqual(len(self.store.list_open_responsibilities(task_id="T-1")), 3)
        self.assertEqual(self.store.list_open_responsibilities(task_id="T-none"), [])
        row = self.store.get_responsibility(b)
        self.ledger.transition_task("T-1", "cancelled", actor="test")
        self.store.close_responsibility(b, expected_revision=row["revision"],
                                        reason="task-cancelled", actor="test")
        self.assertNotIn(b, [r["id"] for r in self.store.list_open_responsibilities()])

    def test_list_due_only_returns_due_and_open(self):
        due = self.rid(condition_key="ck-due", next_check_at=T0 + 1_000)
        self.rid(condition_key="ck-later", next_check_at=T0 + 900_000)
        rows = self.store.list_due_responsibilities(now=T0 + 5_000)
        self.assertEqual([r["id"] for r in rows], [due])
        self.assertEqual(self.store.list_due_responsibilities(now=T0 - 1), [])

    def test_limit_is_bounded_and_validated(self):
        for i in range(3):
            self.rid(condition_key=f"ck-{i}")
        self.assertEqual(len(self.store.list_open_responsibilities(limit=2)), 2)
        for bad in (0, -1, True, "5"):
            with self.subTest(limit=bad):
                with self.assertRaises(LedgerError) as ctx:
                    self.store.list_open_responsibilities(limit=bad)
                self.assertEqual(ctx.exception.code, "RESPONSIBILITY_INVALID")

    def test_get_unknown_returns_none(self):
        self.assertIsNone(self.store.get_responsibility("R-nope"))

    def test_open_page_is_keyset_ordered_and_exclusive(self):
        ids = []
        for i in range(5):
            ids.append(self.store.open_responsibility(
                **_defaults(condition_key=f"ck-{i}"))["id"])
        ordered = sorted(ids)
        first = self.store.list_open_responsibilities_page(limit=2)
        self.assertEqual([r["id"] for r in first], ordered[:2])
        second = self.store.list_open_responsibilities_page(
            after_id=first[-1]["id"], limit=2)
        self.assertEqual([r["id"] for r in second], ordered[2:4])
        third = self.store.list_open_responsibilities_page(
            after_id=second[-1]["id"], limit=2)
        self.assertEqual([r["id"] for r in third], ordered[4:])
        self.assertEqual(self.store.list_open_responsibilities_page(
            after_id=third[-1]["id"], limit=2), [], "尾页返回空，游标可回绕")
        # 回绕后从头再来
        self.assertEqual([r["id"] for r in self.store.list_open_responsibilities_page(
            after_id=None, limit=2)], ordered[:2])
        for bad in ("", 5):
            with self.subTest(after_id=bad):
                with self.assertRaises(LedgerError) as ctx:
                    self.store.list_open_responsibilities_page(after_id=bad)
                self.assertEqual(ctx.exception.code, "RESPONSIBILITY_INVALID")

    def test_open_page_filters_session_and_excludes_closed(self):
        a = self.store.open_responsibility(**_defaults(condition_key="ck-a"))["id"]
        b = self.store.open_responsibility(
            **_defaults(condition_key="ck-b", session_id="ses_worker"))["id"]
        rows = self.store.list_open_responsibilities_page(session_id="ses_worker")
        self.assertEqual([r["id"] for r in rows], sorted([a, b]))
        self.ledger.transition_task("T-1", "cancelled", actor="test")
        self.store.close_responsibility(a, expected_revision=1,
                                        reason="task-cancelled")
        self.assertEqual([r["id"] for r in self.store.list_open_responsibilities_page()],
                         [b])

    def test_unfinished_task_page_is_keyset_and_deserializes_metadata(self):
        t2 = self.ledger.create_task(project="proj", title="t2", task_id="T-2",
                                     metadata={"k": "v"})
        t3 = self.ledger.create_task(project="proj", title="t3", task_id="T-3")
        done = self.ledger.create_task(project="proj", title="t4", task_id="T-4")
        self.ledger.transition_task("T-4", "cancelled", actor="test")
        rows = self.store.list_unfinished_tasks_page(limit=10)
        ids = [r["id"] for r in rows]
        self.assertEqual(ids, sorted([self.task["id"], t2["id"], t3["id"]]))
        self.assertNotIn(done["id"], ids)
        self.assertIsInstance(rows[0]["metadata"], dict)
        self.assertIsInstance(rows[0]["acceptance"], list)
        self.assertIsInstance(rows[0]["write_scope"], list)
        # keyset 前进且不重叠
        first = self.store.list_unfinished_tasks_page(limit=2)
        rest = self.store.list_unfinished_tasks_page(after_id=first[-1]["id"],
                                                     limit=10)
        self.assertFalse({r["id"] for r in first} & {r["id"] for r in rest})
        self.assertEqual(len(first) + len(rest), 3)

    def test_unfinished_task_page_rejects_bad_cursor(self):
        for bad in ("", 0):
            with self.subTest(after_id=bad):
                with self.assertRaises(LedgerError) as ctx:
                    self.store.list_unfinished_tasks_page(after_id=bad)
                self.assertEqual(ctx.exception.code, "RESPONSIBILITY_INVALID")


class TestUpdate(RespBase):
    def setUp(self):
        super().setUp()
        self.row = self.store.open_responsibility(**_defaults())
        self.rid_ = self.row["id"]

    def test_update_bumps_revision_and_cas_rejects_old_snapshot(self):
        updated = self.store.update_responsibility(
            self.rid_, expected_revision=1,
            fields={"next_check_at": T0 + 120_000, "next_action": "再核一次"})
        self.assertEqual(updated["revision"], 2)
        self.assertEqual(updated["next_action"], "再核一次")
        with self.assertRaises(LedgerError) as ctx:
            self.store.update_responsibility(
                self.rid_, expected_revision=1, fields={"next_action": "stale"})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_REVISION_CONFLICT")
        self.assertEqual(self.store.get_responsibility(self.rid_)["next_action"],
                         "再核一次")

    def test_update_rejects_owner_identity_and_unknown_fields(self):
        for fields in ({"owner": "someone-else"}, {"task_id": "T-2"},
                       {"attempt_id": None}, {"generation": 2},
                       {"result_version": "rv1-new"}, {"condition_key": "ck2"},
                       {"revision": 99}, {"closed_at": T0}, {"id": "R-x"}):
            with self.subTest(fields=fields):
                with self.assertRaises(LedgerError) as ctx:
                    self.store.update_responsibility(
                        self.rid_, expected_revision=1, fields=fields)
                self.assertEqual(ctx.exception.code, "RESPONSIBILITY_INVALID")
        with self.assertRaises(LedgerError):
            self.store.update_responsibility(self.rid_, expected_revision=1, fields={})

    def test_update_cannot_enter_closed_state(self):
        for state in ("done", "cancelled", "handed_over"):
            with self.subTest(state=state):
                with self.assertRaises(LedgerError) as ctx:
                    self.store.update_responsibility(
                        self.rid_, expected_revision=1, fields={"state": state})
                self.assertEqual(ctx.exception.code, "RESPONSIBILITY_INVALID")

    def test_budget_monotonic_and_no_automatic_increase(self):
        up = self.store.update_responsibility(
            self.rid_, expected_revision=1, fields={"budget_used": 2})
        self.assertEqual(up["budget_used"], 2)
        with self.assertRaises(LedgerError) as ctx:
            self.store.update_responsibility(
                self.rid_, expected_revision=2, fields={"budget_used": 1})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_INVALID")
        with self.assertRaises(LedgerError) as ctx2:
            self.store.update_responsibility(
                self.rid_, expected_revision=2, fields={"budget_used": 4})
        self.assertEqual(ctx2.exception.code, "RESPONSIBILITY_BUDGET_EXHAUSTED")
        with self.assertRaises(LedgerError) as ctx3:
            self.store.update_responsibility(
                self.rid_, expected_revision=2, fields={"max_budget": 9})
        self.assertEqual(ctx3.exception.code, "RESPONSIBILITY_BUDGET_INCREASE_REFUSED")
        self.assertEqual(self.store.get_responsibility(self.rid_)["max_budget"], 3)
        # 允许下调 max_budget（收紧预算），但不得低于已用
        down = self.store.update_responsibility(
            self.rid_, expected_revision=2, fields={"max_budget": 2})
        self.assertEqual(down["max_budget"], 2)

    def test_closed_responsibility_is_terminal_for_updates(self):
        self.store.update_responsibility(
            self.rid_, expected_revision=1, fields={"state": STATE_WAITING_HUMAN})
        self.ledger.transition_task("T-1", "cancelled", actor="test")
        self.store.close_responsibility(self.rid_, expected_revision=2,
                                        reason="task-cancelled", actor="test")
        with self.assertRaises(LedgerError) as ctx:
            self.store.update_responsibility(
                self.rid_, expected_revision=3, fields={"next_action": "x"})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_TERMINAL")

    def test_paused_task_blocks_progress_updates(self):
        self.ledger.transition_task("T-1", "paused", actor="op")
        with self.assertRaises(LedgerError) as ctx:
            self.store.update_responsibility(
                self.rid_, expected_revision=1, fields={"state": STATE_HANDLING})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_TASK_PAUSED")
        ok = self.store.update_responsibility(
            self.rid_, expected_revision=1, fields={"state": STATE_WAITING_HUMAN})
        self.assertEqual(ok["state"], STATE_WAITING_HUMAN)

    def test_manual_wait_cannot_be_released_by_plain_update(self):
        human = self.store.open_responsibility(**_defaults(
            condition_key="ck-human", state=STATE_WAITING_HUMAN,
            wait_kind="human"))
        for target in (STATE_HANDLING, STATE_WAITING_EXECUTION,
                       STATE_WAITING_INPUT, STATE_AWAITING_REVIEW):
            with self.subTest(target=target):
                with self.assertRaises(LedgerError) as ctx:
                    self.store.update_responsibility(
                        human["id"], expected_revision=1,
                        fields={"state": target}, actor="supervisor")
                self.assertEqual(ctx.exception.code, "RESPONSIBILITY_MANUAL_HOLD")
        self.assertEqual(self.store.get_responsibility(human["id"])["state"],
                         STATE_WAITING_HUMAN)
        # 显式确认 + 明确 resume 接口才可离开人工等待
        resumed = self.store.resume_responsibility(
            human["id"], expected_revision=1, confirm=human["id"],
            reason="operator decided to continue", actor="operator")
        self.assertEqual(resumed["state"], STATE_HANDLING)
        with self.assertRaises(LedgerError) as ctx2:
            self.store.resume_responsibility(
                human["id"], expected_revision=2, confirm="wrong-id",
                reason="x")
        self.assertEqual(ctx2.exception.code, "RESPONSIBILITY_INVALID")

    def test_superseded_generation_cannot_progress(self):
        self.new_attempt()
        with self.assertRaises(LedgerError) as ctx:
            self.store.update_responsibility(
                self.rid_, expected_revision=1, fields={"state": STATE_HANDLING})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_STALE")
        ok = self.store.update_responsibility(
            self.rid_, expected_revision=1, fields={"state": STATE_WAITING_HUMAN})
        self.assertEqual(ok["state"], STATE_WAITING_HUMAN)


class TestTransfer(RespBase):
    def _successor(self, **over):
        base = {"owner": "ops", "target_session": "ses_human",
                "state": STATE_WAITING_HUMAN, "wait_kind": "human-decision",
                "wait_condition": "decision", "next_action": "decide"}
        base.update(over)
        return base

    def test_transfer_links_previous_and_successor_without_losing_duty(self):
        row = self.store.open_responsibility(**_defaults())
        out = self.store.transfer_responsibility(
            row["id"], expected_revision=1, successor=self._successor(),
            reason="handover-to-human", actor="coord")
        prev, succ = out["previous"], out["successor"]
        self.assertEqual(prev["state"], "handed_over")
        self.assertIsNotNone(prev["closed_at"])
        self.assertEqual(prev["close_reason"], "handover-to-human")
        self.assertEqual(prev["successor_id"], succ["id"])
        self.assertEqual(succ["prior_id"], prev["id"])
        self.assertEqual(succ["owner"], "ops")
        self.assertEqual(succ["target_session"], "ses_human")
        self.assertIsNone(succ["closed_at"])
        self.assertEqual(len(self.store.list_open_responsibilities()), 1)

    def test_second_connection_old_revision_transfer_is_rejected_idempotently(self):
        row = self.store.open_responsibility(**_defaults())
        ledger2, store2 = self.second()
        first = self.store.transfer_responsibility(
            row["id"], expected_revision=1, successor=self._successor(),
            reason="handover")
        before = self.conn.execute(
            "SELECT COUNT(*) FROM responsibilities").fetchone()[0]
        with self.assertRaises(LedgerError) as ctx:
            store2.transfer_responsibility(
                row["id"], expected_revision=1,
                successor=self._successor(condition_key="duplicate-review"),
                reason="late observer")
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_TERMINAL")
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM responsibilities").fetchone()[0], before)
        open_rows = self.store.list_open_responsibilities(task_id="T-1")
        self.assertEqual([r["id"] for r in open_rows], [first["successor"]["id"]])
        self.assertEqual(store2.get_responsibility(
            first["successor"]["id"])["prior_id"], row["id"])

    def test_transfer_rolls_back_when_successor_invalid(self):
        row = self.store.open_responsibility(**_defaults())
        with self.assertRaises(LedgerError):
            self.store.transfer_responsibility(
                row["id"], expected_revision=1,
                successor=self._successor(wait_kind=""), reason="handover")
        fresh = self.store.get_responsibility(row["id"])
        self.assertIsNone(fresh["closed_at"])
        self.assertIsNone(fresh["successor_id"])
        self.assertEqual(fresh["revision"], 1)
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM responsibilities").fetchone()[0], 1)

    def test_transfer_rejects_terminal_task_and_double_transfer(self):
        row = self.store.open_responsibility(**_defaults())
        out = self.store.transfer_responsibility(
            row["id"], expected_revision=1, successor=self._successor(),
            reason="handover")
        with self.assertRaises(LedgerError) as ctx:
            self.store.transfer_responsibility(
                row["id"], expected_revision=2, successor=self._successor(),
                reason="again")
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_TERMINAL")
        self.ledger.transition_task("T-1", "cancelled", actor="test")
        with self.assertRaises(LedgerError) as ctx2:
            self.store.transfer_responsibility(
                out["successor"]["id"], expected_revision=1,
                successor=self._successor(), reason="handover")
        self.assertEqual(ctx2.exception.code, "RESPONSIBILITY_TASK_TERMINAL")

    def test_transfer_cannot_reset_budget_or_jump_task(self):
        row = self.store.open_responsibility(**_defaults())
        row = self.store.update_responsibility(
            row["id"], expected_revision=1, fields={"budget_used": 2})
        other = self.ledger.create_task(project="proj", title="other",
                                        task_id="T-2")
        for over, code in (
            ({"budget_used": 0}, "RESPONSIBILITY_TRANSFER_BUDGET_REFUSED"),
            ({"max_budget": 999}, "RESPONSIBILITY_TRANSFER_BUDGET_REFUSED"),
            ({"task_id": other["id"]}, "RESPONSIBILITY_TRANSFER_SCOPE_REFUSED"),
            ({"project": "other-project"},
             "RESPONSIBILITY_TRANSFER_SCOPE_REFUSED"),
            ({"deadline": T0 + 9_999_999},
             "RESPONSIBILITY_TRANSFER_DEADLINE_REFUSED"),
            ({"result_version": "rv1-forged"},
             "RESPONSIBILITY_TRANSFER_IDENTITY_REFUSED"),
        ):
            with self.subTest(over=over):
                with self.assertRaises(LedgerError) as ctx:
                    self.store.transfer_responsibility(
                        row["id"], expected_revision=2,
                        successor=self._successor(**over), reason="handoff")
                self.assertEqual(ctx.exception.code, code)
        self.assertEqual(self.store.get_responsibility(row["id"])["budget_used"], 2)

    def test_transfer_keeps_manual_hold_unless_confirmed(self):
        human = self.store.open_responsibility(**_defaults(
            condition_key="ck-human", state=STATE_WAITING_HUMAN,
            wait_kind="human"))
        with self.assertRaises(LedgerError) as ctx:
            self.store.transfer_responsibility(
                human["id"], expected_revision=1,
                successor=self._successor(state=STATE_AWAITING_REVIEW),
                reason="handoff")
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_MANUAL_HOLD")
        ok = self.store.transfer_responsibility(
            human["id"], expected_revision=1,
            successor=self._successor(state=STATE_WAITING_HUMAN),
            reason="handoff")
        self.assertEqual(ok["successor"]["state"], STATE_WAITING_HUMAN)
        # 显式确认后允许转到活跃责任
        human2 = self.store.open_responsibility(**_defaults(
            condition_key="ck-human-2", state=STATE_WAITING_HUMAN,
            wait_kind="human"))
        moved = self.store.transfer_responsibility(
            human2["id"], expected_revision=1,
            successor=self._successor(state=STATE_AWAITING_REVIEW),
            reason="handoff", release_manual_confirmation=human2["id"])
        self.assertEqual(moved["successor"]["state"], STATE_AWAITING_REVIEW)


class TestClose(RespBase):
    def test_reason_text_cannot_close_unfinished_task(self):
        row = self.store.open_responsibility(**_defaults())
        for reason in ("manual-close", "duplicate", "transferred",
                       "superseded:operator-says-so", "notification-acked",
                       "execution-succeeded", "inbox-empty"):
            with self.subTest(reason=reason):
                with self.assertRaises(LedgerError) as ctx:
                    self.store.close_responsibility(
                        row["id"], expected_revision=1, reason=reason)
                self.assertEqual(ctx.exception.code, "RESPONSIBILITY_CLOSE_REFUSED")
        self.assertIsNone(self.store.get_responsibility(row["id"])["closed_at"])

    def test_close_allowed_when_task_really_accepted(self):
        human = self.store.open_responsibility(**_defaults(
            condition_key="ck-human", state=STATE_WAITING_HUMAN,
            wait_kind="human"))
        self.mark_task_accepted()
        out = self.store.close_responsibility(
            human["id"], expected_revision=1, reason="task-accepted",
            actor="supervisor")
        self.assertEqual(out["state"], "done")
        self.assertIsNotNone(out["closed_at"])
        self.assertEqual(out["close_reason"], "task-accepted")

    def test_close_allowed_when_task_cancelled(self):
        row = self.store.open_responsibility(**_defaults())
        self.ledger.transition_task("T-1", "cancelled", actor="test")
        out = self.store.close_responsibility(
            row["id"], expected_revision=1, reason="task-cancelled")
        self.assertEqual(out["state"], "cancelled")

    def test_closed_at_alone_is_not_accepted_evidence(self):
        row = self.store.open_responsibility(**_defaults())
        self.conn.execute("UPDATE tasks SET closed_at=? WHERE id='T-1'", (T0,))
        with self.assertRaises(LedgerError) as ctx:
            self.store.close_responsibility(
                row["id"], expected_revision=1, reason="task-accepted")
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_CLOSE_REFUSED")
        self.assertIsNone(self.store.get_responsibility(row["id"])["closed_at"])

    def test_missing_task_becomes_visible_waiting_human(self):
        row = self.store.open_responsibility(**_defaults())
        # 模拟损坏引用：测试夹具临时关 FK 后删除任务（生产不会出现该写入路径）
        self.conn.execute("PRAGMA foreign_keys=OFF")
        self.addCleanup(self.conn.execute, "PRAGMA foreign_keys=ON")
        self.conn.execute("DELETE FROM tasks WHERE id='T-1'")
        out = self.store.close_responsibility(
            row["id"], expected_revision=1, reason="task-accepted")
        self.assertIsNone(out["closed_at"], "缺 task 不得当 terminal 成功关闭")
        self.assertEqual(out["state"], STATE_WAITING_HUMAN)
        self.assertEqual(out["revision"], 2)
        self.assertTrue(any(a["kind"] == "responsibility_blocked"
                            for a in self.alerts()))

    def test_superseded_by_requires_verified_replacement(self):
        old = self.store.open_responsibility(**_defaults())
        # 同任务、仍绑定旧 attempt 的“后继”不是有效替代
        same_attempt = self.store.open_responsibility(**_defaults(
            condition_key="ck-peer", attempt_id="A-1", generation=1))
        with self.assertRaises(LedgerError) as ctx:
            self.store.close_responsibility(
                old["id"], expected_revision=1, reason="superseded",
                superseded_by=same_attempt["id"])
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_CLOSE_REFUSED")
        # 真正的新代次责任才可作为替代
        self.new_attempt()
        replacement = self.store.open_responsibility(**_defaults(
            condition_key="ck-new", attempt_id="A-2", generation=2,
            session_id="ses_worker2"))
        out = self.store.close_responsibility(
            old["id"], expected_revision=1, reason="superseded:new-attempt",
            superseded_by=replacement["id"])
        self.assertEqual(out["state"], "handed_over")
        self.assertEqual(out["successor_id"], replacement["id"])
        self.assertEqual(out["close_reason"], "superseded:new-attempt")

    def test_superseded_by_other_task_or_closed_is_refused(self):
        old = self.store.open_responsibility(**_defaults())
        other = self.ledger.create_task(project="proj", title="other",
                                        task_id="T-2")
        self.ledger.create_attempt("T-2", attempt_id="A-other",
                                   directory=self.home, session_id="ses_x",
                                   generation=1, status="running",
                                   takeover_confirmed=True)
        foreign = self.store.open_responsibility(**_defaults(
            task_id="T-2", attempt_id="A-other", generation=1,
            session_id="ses_x", condition_key="ck-foreign"))
        with self.assertRaises(LedgerError) as ctx:
            self.store.close_responsibility(
                old["id"], expected_revision=1, reason="superseded",
                superseded_by=foreign["id"])
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_CLOSE_REFUSED")

    def test_close_cas_conflict_does_not_close_new_revision(self):
        row = self.store.open_responsibility(**_defaults())
        self.store.update_responsibility(row["id"], expected_revision=1,
                                         fields={"next_action": "new"})
        with self.assertRaises(LedgerError) as ctx:
            self.store.close_responsibility(
                row["id"], expected_revision=1, reason="manual-close")
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_REVISION_CONFLICT")
        self.assertIsNone(self.store.get_responsibility(row["id"])["closed_at"])


class TestPublish(RespBase):
    def test_publish_requires_verifiable_review_fact(self):
        row = self.store.open_responsibility(**_defaults(
            state=STATE_AWAITING_REVIEW, evidence={"source": "test"}))
        with self.assertRaises(LedgerError) as ctx:
            self.store.publish_responsibility(
                row["id"], expected_revision=1, payload={"summary": "x"})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_NOT_REVIEWABLE")
        self.assertEqual(self.notif_count(), 0)
        self.assertEqual(self.store.get_responsibility(row["id"])["revision"], 1,
                         "无事实不得改动状态")

    def test_publish_normalizes_state_and_creates_notification_in_one_tx(self):
        row = self.store.open_responsibility(**_defaults(
            state=STATE_WAITING_INPUT, evidence=_review_evidence()))
        out = self.store.publish_responsibility(
            row["id"], expected_revision=1,
            payload={"summary": "worker 缺 baseline 材料"})
        self.assertTrue(out["published"])
        self.assertEqual(out["state"], STATE_AWAITING_REVIEW,
                         "published state 必须与通知同事务落库")
        self.assertEqual(out["revision"], 2)
        notif = out["notification"]
        self.assertEqual(notif["kind"], "continuation_ready")
        self.assertEqual(notif["target_session"], "ses_coord")
        self.assertEqual(
            notif["dedupe_key"], f"resp:{row['id']}:{row['condition_key']}:g1.r-")
        self.assertNotIn(str(T0), notif["dedupe_key"])
        payload = notif["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        for key in ("responsibility_id", "condition_key", "condition_version",
                    "next_action", "owner", "wait_kind", "wait_condition",
                    "review_fact"):
            self.assertIn(key, payload)
        self.assertEqual(payload["review_fact"], "execution-ended")

    def test_publish_accepts_missing_responsibility_diagnostic(self):
        row = self.store.open_responsibility(**_defaults(
            state=STATE_AWAITING_REVIEW, condition_key=MISSING_CONDITION_KEY,
            evidence={"reason": "no-open-responsibility", "reconciles": 2}))
        out = self.store.publish_responsibility(
            row["id"], expected_revision=1, payload={"summary": "missing"})
        self.assertTrue(out["published"])
        self.assertEqual(out["notification"]["kind"], "continuation_ready")

    def test_republish_is_idempotent_even_after_ack(self):
        row = self.review_rid()
        first = self.store.publish_responsibility(
            row, expected_revision=1, payload={"summary": "a"})
        fresh = self.store.get_responsibility(row)
        second = self.store.publish_responsibility(
            row, expected_revision=fresh["revision"], payload={"summary": "b"})
        self.assertFalse(second["published"])
        self.assertEqual(first["notification"]["id"], second["notification"]["id"])
        self.assertEqual(self.notif_count(), 1)
        self.ledger.ack_notifications([first["notification"]["id"]], actor="coord")
        newer = self.store.get_responsibility(row)
        third = self.store.publish_responsibility(
            row, expected_revision=newer["revision"], payload={"summary": "c"})
        self.assertFalse(third["published"], "ACK 后同一条件不得反复新建通知")
        self.assertEqual(self.notif_count(), 1)

    def test_no_route_marks_waiting_human_with_deduped_alert(self):
        self.conn.execute("UPDATE tasks SET coordinator_session=NULL WHERE id='T-1'")
        resp = self.store.open_responsibility(**_defaults(
            target_session=None, evidence=_review_evidence()))
        out = self.store.publish_responsibility(
            resp["id"], expected_revision=1, payload={"summary": "x"})
        self.assertFalse(out["published"])
        self.assertEqual(out["reason"], "no-route")
        self.assertIsNone(out["notification"])
        self.assertEqual(out["state"], STATE_WAITING_HUMAN)
        self.assertEqual(self.notif_count(), 0)
        alerts = [a for a in self.alerts() if a["kind"] == "responsibility_blocked"]
        self.assertEqual(len(alerts), 1)
        with self.assertRaises(LedgerError):
            self.store.publish_responsibility(
                resp["id"], expected_revision=out["revision"],
                payload={"summary": "x"})
        self.assertEqual(len([a for a in self.alerts()
                              if a["kind"] == "responsibility_blocked"]), 1)

    def test_task_coordinator_overrides_stale_stored_target(self):
        resp = self.store.open_responsibility(**_defaults(
            target_session="ses_stale", evidence=_review_evidence()))
        out = self.store.publish_responsibility(
            resp["id"], expected_revision=1, payload={})
        self.assertEqual(out["target"], "ses_coord")
        self.assertEqual(out["reason"], "task-coordinator-override")

    def test_lease_holder_is_used_when_task_has_no_coordinator(self):
        self.conn.execute("UPDATE tasks SET coordinator_session=NULL WHERE id='T-1'")
        self.ledger.claim_coordinator(project="proj", session_id="ses_holder",
                                      actor="op", ttl=300)
        resp = self.store.open_responsibility(**_defaults(
            target_session=None, evidence=_review_evidence()))
        out = self.store.publish_responsibility(
            resp["id"], expected_revision=1, payload={})
        self.assertTrue(out["published"])
        self.assertEqual(out["target"], "ses_holder")
        self.assertEqual(out["reason"], "lease-holder")

    def test_paused_task_publishes_nothing_and_waits_human(self):
        resp = self.review_rid()
        self.ledger.transition_task("T-1", "paused", actor="op")
        out = self.store.publish_responsibility(
            resp, expected_revision=1, payload={"summary": "x"})
        self.assertFalse(out["published"])
        self.assertEqual(out["reason"], "task-paused")
        self.assertEqual(out["state"], STATE_WAITING_HUMAN)
        self.assertEqual(self.notif_count(), 0)
        self.assertTrue(any(a["kind"] == "responsibility_blocked"
                            for a in self.alerts()))

    def test_terminal_task_cannot_publish(self):
        resp = self.review_rid()
        self.ledger.transition_task("T-1", "cancelled", actor="test")
        with self.assertRaises(LedgerError) as ctx:
            self.store.publish_responsibility(
                resp, expected_revision=1, payload={})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_TASK_TERMINAL")
        self.assertEqual(self.notif_count(), 0)

    def test_deadline_or_budget_exhaustion_escalates_without_publish(self):
        expired = self.store.open_responsibility(**_defaults(
            condition_key="ck-expired", deadline=T0 - 1,
            evidence=_review_evidence()))
        out = self.store.publish_responsibility(
            expired["id"], expected_revision=1, payload={})
        self.assertFalse(out["published"])
        self.assertEqual(out["state"], STATE_WAITING_HUMAN)
        self.assertEqual(self.notif_count(), 0)
        capped = self.store.open_responsibility(**_defaults(
            condition_key="ck-capped", max_budget=1,
            evidence=_review_evidence()))
        self.store.update_responsibility(capped["id"], expected_revision=1,
                                         fields={"budget_used": 1})
        out2 = self.store.publish_responsibility(
            capped["id"], expected_revision=2, payload={})
        self.assertFalse(out2["published"])
        self.assertEqual(out2["state"], STATE_WAITING_HUMAN)
        self.assertEqual(self.notif_count(), 0)

    def test_missing_current_attempt_cannot_progress_or_publish(self):
        resp = self.review_rid(condition_key="ck-nocurrent")
        self.conn.execute("UPDATE tasks SET current_attempt_id=NULL WHERE id='T-1'")
        with self.assertRaises(LedgerError) as ctx:
            self.store.update_responsibility(
                resp, expected_revision=1, fields={"state": STATE_HANDLING})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_STALE")
        with self.assertRaises(LedgerError) as ctx2:
            self.store.publish_responsibility(
                resp, expected_revision=1, payload={"summary": "x"})
        self.assertEqual(ctx2.exception.code, "RESPONSIBILITY_STALE")
        self.assertEqual(self.notif_count(), 0)

    def test_result_version_change_blocks_publish(self):
        self.conn.execute(
            "UPDATE attempts SET metadata=? WHERE id='A-1'",
            (json.dumps({"result": {"version": "rv1-old"}}),))
        resp = self.review_rid(condition_key="ck-ver", result_version="rv1-old")
        self.conn.execute(
            "UPDATE attempts SET metadata=? WHERE id='A-1'",
            (json.dumps({"result": {"version": "rv1-new"}}),))
        with self.assertRaises(LedgerError) as ctx:
            self.store.publish_responsibility(
                resp, expected_revision=1, payload={"summary": "old-version"})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_STALE")
        self.assertIn("result-version", str(ctx.exception))
        self.assertEqual(self.notif_count(), 0)

    def test_continuation_fact_requires_identity_binding(self):
        bare = self.store.open_responsibility(**_defaults(
            condition_key="ck-bare", input_id="I-cont",
            evidence={"continuation": {"kind": "worker-report"}}))
        with self.assertRaises(LedgerError) as ctx:
            self.store.publish_responsibility(
                bare["id"], expected_revision=1, payload={"summary": "x"})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_NOT_REVIEWABLE")
        wrong = self.store.open_responsibility(**_defaults(
            condition_key="ck-wrong", input_id="I-cont",
            evidence={"continuation": {"kind": "worker-report",
                                       "input_id": "I-other"}}))
        with self.assertRaises(LedgerError) as ctx2:
            self.store.publish_responsibility(
                wrong["id"], expected_revision=1, payload={"summary": "x"})
        self.assertEqual(ctx2.exception.code, "RESPONSIBILITY_NOT_REVIEWABLE")
        bound = self.store.open_responsibility(**_defaults(
            condition_key="ck-bound", input_id="I-cont",
            evidence={"continuation": {"kind": "worker-report",
                                       "input_id": "I-cont",
                                       "message_id": "msg_in"}}))
        out = self.store.publish_responsibility(
            bound["id"], expected_revision=1, payload={"summary": "ok"})
        self.assertTrue(out["published"])
        payload = out["notification"]["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        self.assertEqual(payload["review_fact"], "continuation-fact")
        # 以责任 id 绑定也可追溯
        by_rid = self.store.open_responsibility(**_defaults(
            condition_key="ck-rid",
            evidence={"continuation": {"kind": "worker-report",
                                       "responsibility_id": bare["id"]}}))
        with self.assertRaises(LedgerError):
            self.store.publish_responsibility(
                by_rid["id"], expected_revision=1, payload={"summary": "x"})

    def test_superseded_generation_blocks_publish(self):
        resp = self.review_rid(condition_key="ck-pub")
        self.new_attempt()
        with self.assertRaises(LedgerError) as ctx:
            self.store.publish_responsibility(
                resp, expected_revision=1, payload={"summary": "old"})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_STALE")
        self.assertEqual(self.notif_count(), 0)

    def test_publish_cas_conflict(self):
        resp = self.review_rid()
        self.store.update_responsibility(resp, expected_revision=1,
                                         fields={"next_action": "new"})
        with self.assertRaises(LedgerError) as ctx:
            self.store.publish_responsibility(
                resp, expected_revision=1, payload={})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_REVISION_CONFLICT")
        self.assertEqual(self.notif_count(), 0)

    def test_publish_rolls_back_on_db_trigger_abort(self):
        resp = self.review_rid()
        self.conn.execute(
            "CREATE TRIGGER fail_continuation BEFORE INSERT ON notifications "
            "BEGIN SELECT RAISE(ABORT, 'simulated-crash'); END")
        with self.assertRaises(sqlite3.DatabaseError):
            self.store.publish_responsibility(
                resp, expected_revision=1, payload={"summary": "x"})
        fresh = self.store.get_responsibility(resp)
        self.assertEqual((fresh["state"], fresh["revision"]),
                         (STATE_AWAITING_REVIEW, 1))
        self.assertEqual(self.notif_count(), 0)
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM audit WHERE action='responsibility.publish'"
        ).fetchone()[0], 0)
        self.conn.execute("DROP TRIGGER fail_continuation")
        ok = self.store.publish_responsibility(
            resp, expected_revision=1, payload={"summary": "retry"})
        self.assertTrue(ok["published"])

    def test_publish_rolls_back_when_notification_enqueue_fails(self):
        from unittest.mock import patch
        resp = self.review_rid()
        with patch.object(self.ledger, "_enqueue_notification_in_tx",
                          side_effect=RuntimeError("injected-notify-crash")):
            with self.assertRaises(RuntimeError):
                self.store.publish_responsibility(
                    resp, expected_revision=1, payload={"summary": "x"})
        fresh = self.store.get_responsibility(resp)
        self.assertEqual((fresh["state"], fresh["revision"]),
                         (STATE_AWAITING_REVIEW, 1))
        self.assertEqual(self.notif_count(), 0)
        out = self.store.publish_responsibility(
            resp, expected_revision=1, payload={"summary": "retry"})
        self.assertTrue(out["published"])
        self.assertEqual(self.notif_count(), 1)

    def test_ack_in_other_connection_does_not_close_or_block_new_condition(self):
        resp = self.review_rid(condition_key="ck-one")
        first = self.store.publish_responsibility(
            resp, expected_revision=1, payload={"summary": "first"})
        ledger2, store2 = self.second()
        ledger2.ack_notifications([first["notification"]["id"]], actor="coord")
        self.assertIsNone(self.store.get_responsibility(resp)["closed_at"])
        fresh = store2.get_responsibility(resp)
        again = store2.publish_responsibility(
            resp, expected_revision=fresh["revision"], payload={"summary": "again"})
        self.assertFalse(again["published"])
        self.assertEqual(self.notif_count(), 1)
        newer = store2.open_responsibility(**_defaults(
            condition_key="ck-two", evidence=_review_evidence()))
        pub2 = store2.publish_responsibility(
            newer["id"], expected_revision=1, payload={"summary": "new fact"})
        self.assertTrue(pub2["published"])
        self.assertEqual(self.notif_count(), 2)
        self.assertEqual(
            self.ledger.get_notification(first["notification"]["id"])["state"],
            "acked")


class TestPendingNotificationQuery(RespBase):
    def test_counts_only_current_attempt_and_task_level(self):
        self.assertEqual(self.store.has_pending_task_notifications("T-1"), 0)
        self.ledger.enqueue_notification(
            kind="execution_succeeded", target_session="ses_c",
            dedupe_key="k-current", payload={}, project="proj",
            task_id="T-1", attempt_id="A-1")
        self.assertEqual(self.store.has_pending_task_notifications("T-1"), 1)
        task_level = self.ledger.enqueue_notification(
            kind="responsibility_missing", target_session="ses_c",
            dedupe_key="k-task", payload={}, project="proj", task_id="T-1")
        self.assertEqual(self.store.has_pending_task_notifications("T-1"), 2)
        # 旧 attempt 的未清通知不算“当前已有待办”
        self.new_attempt()
        self.assertEqual(self.store.has_pending_task_notifications("T-1"), 1)
        # 已 ACK 不计数
        self.ledger.ack_notifications([task_level["id"]], actor="test")
        self.assertEqual(self.store.has_pending_task_notifications("T-1"), 0)

    def test_sent_and_inflight_count_and_result_version_filter(self):
        note = self.ledger.enqueue_notification(
            kind="result_ready", target_session="ses_c", dedupe_key="k-v",
            payload={"result_version": "rv1-1"}, project="proj",
            task_id="T-1", attempt_id="A-1")
        self.assertEqual(self.store.has_pending_task_notifications(
            "T-1", result_version="rv1-1"), 1)
        self.assertEqual(self.store.has_pending_task_notifications(
            "T-1", result_version="rv1-2"), 0)
        self.conn.execute(
            "UPDATE notifications SET state='sent' WHERE id=?", (note["id"],))
        self.assertEqual(self.store.has_pending_task_notifications("T-1"), 1)
        self.conn.execute(
            "UPDATE notifications SET state='inflight' WHERE id=?", (note["id"],))
        self.assertEqual(self.store.has_pending_task_notifications("T-1"), 1)
        self.conn.execute(
            "UPDATE notifications SET state='dead' WHERE id=?", (note["id"],))
        self.assertEqual(self.store.has_pending_task_notifications("T-1"), 0)

    def test_query_rejects_bad_input(self):
        with self.assertRaises(LedgerError) as ctx:
            self.store.has_pending_task_notifications("T-missing")
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_TASK_NOT_FOUND")
        with self.assertRaises(LedgerError):
            self.store.has_pending_task_notifications("")


class TestConcurrencyAndRestart(RespBase):
    def test_two_connections_open_same_condition_yields_one_row(self):
        barrier = threading.Barrier(2)
        results = {}

        def work(name):
            conn = connect(self.home)
            try:
                store = ResponsibilityStore(Ledger(conn, self.home, clock=self.clock))
                barrier.wait()
                out = store.open_responsibility(**_defaults())
                results[name] = ("ok", out["created"], out["id"])
            except Exception as exc:  # noqa: BLE001
                results[name] = ("err", type(exc).__name__, str(exc)[:160])
            finally:
                conn.close()

        threads = [threading.Thread(target=work, args=("a",)),
                   threading.Thread(target=work, args=("b",))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(r[0] for r in results.values()), ["ok", "ok"], results)
        self.assertEqual(len({results["a"][2], results["b"][2]}), 1)
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM responsibilities").fetchone()[0], 1)
        self.assertEqual(sorted(bool(results[k][1]) for k in ("a", "b")),
                         [False, True])

    def test_two_connections_same_revision_only_one_update_wins(self):
        row = self.store.open_responsibility(**_defaults())
        barrier = threading.Barrier(2)
        results = {}

        def work(name):
            conn = connect(self.home)
            try:
                store = ResponsibilityStore(Ledger(conn, self.home, clock=self.clock))
                barrier.wait()
                out = store.update_responsibility(
                    row["id"], expected_revision=1,
                    fields={"next_action": f"by-{name}"})
                results[name] = ("ok", out["next_action"], out["revision"])
            except LedgerError as exc:
                results[name] = ("err", exc.code)
            except Exception as exc:  # noqa: BLE001
                results[name] = ("crash", type(exc).__name__, str(exc)[:160])
            finally:
                conn.close()

        threads = [threading.Thread(target=work, args=("a",)),
                   threading.Thread(target=work, args=("b",))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        winners = [r for r in results.values() if r[0] == "ok"]
        self.assertEqual(len(winners), 1, results)
        self.assertEqual(winners[0][2], 2)
        self.assertEqual([r for r in results.values() if r[0] == "err"][0][1],
                         "RESPONSIBILITY_REVISION_CONFLICT")
        self.assertEqual(self.store.get_responsibility(row["id"])["next_action"],
                         winners[0][1])

    def test_budget_and_schedule_survive_restart(self):
        row = self.store.open_responsibility(**_defaults())
        self.store.update_responsibility(row["id"], expected_revision=1,
                                         fields={"budget_used": 2,
                                                 "next_check_at": T0 + 42_000})
        ledger2, store2 = self.second()
        again = store2.get_responsibility(row["id"])
        self.assertEqual((again["budget_used"], again["next_check_at"]),
                         (2, T0 + 42_000))
        with self.assertRaises(LedgerError) as ctx:
            store2.update_responsibility(row["id"], expected_revision=2,
                                         fields={"budget_used": 0})
        self.assertEqual(ctx.exception.code, "RESPONSIBILITY_INVALID")
        due = store2.list_due_responsibilities(now=T0 + 43_000)
        self.assertEqual([r["id"] for r in due], [row["id"]])

    def test_audit_trail_records_open_update_transfer_close(self):
        row = self.store.open_responsibility(**_defaults())
        self.store.update_responsibility(row["id"], expected_revision=1,
                                         fields={"next_action": "u"})
        out = self.store.transfer_responsibility(
            row["id"], expected_revision=2,
            successor={"owner": "ops", "wait_kind": "k", "wait_condition": "c",
                       "next_action": "n", "state": STATE_WAITING_HUMAN},
            reason="handover")
        self.ledger.transition_task("T-1", "cancelled", actor="test")
        self.store.close_responsibility(
            out["successor"]["id"], expected_revision=1, reason="task-cancelled")
        actions = [r["action"] for r in self.conn.execute(
            "SELECT action FROM audit WHERE entity='responsibility' ORDER BY id")]
        self.assertEqual(actions, ["responsibility.open", "responsibility.update",
                                   "responsibility.transfer",
                                   "responsibility.close"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

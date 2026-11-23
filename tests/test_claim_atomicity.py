#!/usr/bin/env python3
"""F1 切片：create_attempt 写权认领原子性回归（OpenSpec modularize-orchestrator 1.1 / 2.1）。

保护的缺陷（基点 a6d8eeb）：create_attempt 的任务状态检查、generation 计算与
scope 冲突检查全部发生在 BEGIN IMMEDIATE 之外；两条并发认领会各自读到空快照，
从而在真实 WAL 库上同时持有重叠写范围。

时序设计（稳定红→绿，不依赖概率 sleep）：
- 只包装目标连接（worker 线程独占连接）对 ledger.transaction 的首次调用，
  在真实 BEGIN IMMEDIATE 之前用 Event 暂停 worker；其余连接原样通过。
  worker 暂停期间不持有任何锁，因此不会与主线程互等。
- 主线程用第二个连接真实提交竞争状态（claim / generation / cancel /
  stop_requested），然后释放 worker。
- 旧实现已从旧快照完成全部检查，会放行（红）；新实现先等事务再读，会拒绝（绿）。
同一用例无需修改即可作为修复后回归。
"""
from __future__ import annotations

import contextlib
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

import oc_orch.db as db_mod
import oc_orch.ledger as ledger_mod
from oc_orch.db import init
from oc_orch.ledger import Ledger, LedgerError
from oc_orch.util import json_loads, paths_overlap

GATE_TIMEOUT = 30.0


class ClaimGate:
    """在目标连接首次调用 ledger.transaction 时、执行真实 BEGIN 之前暂停 worker。

    - 用 unittest.mock.patch 包装，但不伪造任何查询结果，只控制时序。
    - 只拦截通过 arm() 登记的连接对象；主线程连接与其余调用原样直通。
    """

    def __init__(self, timeout: float = GATE_TIMEOUT):
        self.timeout = timeout
        self.target_conn = None
        self.entered = threading.Event()   # worker 已到事务门口（尚未 BEGIN）
        self.release = threading.Event()   # 主线程放行
        self._ctx = None
        self._lock = threading.RLock()
        self._seen = False

    def install(self) -> "ClaimGate":
        real = db_mod.transaction
        gate = self

        @contextlib.contextmanager
        def gated(conn, *a, **kw):
            with gate._lock:
                if conn is gate.target_conn and not gate._seen:
                    gate._seen = True
                    intercept = True
                else:
                    intercept = False
            if intercept:
                gate.entered.set()
                if not gate.release.wait(gate.timeout):
                    raise AssertionError("ClaimGate: worker 等待放行超时")
            with real(conn, *a, **kw):
                yield

        self._ctx = mock.patch.object(ledger_mod, "transaction", gated)
        self._ctx.start()
        return self

    def uninstall(self) -> None:
        if self._ctx is not None:
            self._ctx.stop()
            self._ctx = None

    def arm(self, conn) -> None:
        """worker 在自己的连接上调用；目标连接以对象身份精确匹配。"""
        with self._lock:
            self.target_conn = conn
            self._seen = False
        self.entered.clear()
        self.release.clear()


def claim_worker_run(home: str, task_id: str, gate: ClaimGate,
                     attempt_id: str, **claim_kwargs) -> dict:
    """在独立线程/独立连接上执行 create_attempt，只把结果带回主线程。

    finally 中总是关闭连接；失败后在同一连接上再执行一次真实事务写入，
    以验证回滚后连接仍可用（不向主线程抛异常）。
    """
    conn = init(home)
    ledger = Ledger(conn, home)
    gate.arm(conn)
    wr = {"attempt_id": attempt_id}
    try:
        wr["attempt"] = ledger.create_attempt(task_id, attempt_id=attempt_id,
                                              **claim_kwargs)
    except BaseException as e:  # noqa: BLE001 - 结果交给主线程断言
        wr["error"] = e
    finally:
        try:
            ledger.set_meta("f1.conn_usable", "ok")
            wr["conn_usable"] = True
        except BaseException as e:  # noqa: BLE001
            wr["conn_usable"] = False
            wr["conn_usable_error"] = repr(e)
        conn.close()
    return wr


def count_overlap_holders(conn, scope) -> int:
    """当前未确认释放（site_released_at IS NULL）且与 scope 重叠的 attempt 数。"""
    rows = conn.execute(
        "SELECT * FROM attempts WHERE site_released_at IS NULL"
    ).fetchall()
    n = 0
    for r in rows:
        att = dict(r)
        meta = json_loads(att.get("metadata"), {})
        other = meta.get("write_scope") or [att["directory"]]
        if any(paths_overlap(a, b) for a in scope for b in other):
            n += 1
    return n


class _FakeClock:
    """线程安全的可推进时钟，通过 mock.patch('oc_orch.ledger.now_ms', new=...) 注入。

    只推进、不 sleep：主线程在竞争提交前/释放 worker 前各 advance 一次，
    worker 线程读到的 now_ms 即为当前值（与真实 SQLite 无关，仅驱动 created_at）。
    """

    def __init__(self, start: int = 1000000):
        self._t = start
        self._lock = threading.Lock()

    def __call__(self) -> int:
        with self._lock:
            return self._t

    def advance(self, step: int = 1000) -> None:
        with self._lock:
            self._t += step


class ClaimAtomicityTest(unittest.TestCase):

    def setUp(self):
        # dir 不硬编码：遵循默认 tempfile 规则，使 scripts/check_offline.py 注入的
        # TMPDIR 生效、干净环境可运行；cleanup 按 LIFO 注册（先关连接再删目录，
        # 且 init 失败时目录也能被清理）。
        self._tmp = tempfile.TemporaryDirectory(prefix="oc-orch-f1-")
        self.addCleanup(self._tmp.cleanup)
        self.home = self._tmp.name
        self.conn = init(self.home)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.home)

    # ---- 并发编排 -------------------------------------------------
    def run_race(self, task_id, attempt_id, compete, **claim_kwargs):
        """worker 在事务门口暂停；主线程完成 compete 真实提交后再放行。

        返回 (worker 结果 dict, compete 输出 dict)。
        """
        gate = ClaimGate()
        gate.install()
        wr: dict = {}

        def worker():
            wr.update(claim_worker_run(self.home, task_id, gate, attempt_id,
                                       **claim_kwargs))

        out: dict = {}
        th = threading.Thread(target=worker, name="f1-claim", daemon=True)
        th.start()
        try:
            self.assertTrue(
                gate.entered.wait(GATE_TIMEOUT),
                "worker 未到达 create_attempt 的事务门口（前置检查提前失败）",
            )
            compete(out)
        finally:
            gate.release.set()
            th.join(GATE_TIMEOUT)
            gate.uninstall()
        self.assertFalse(th.is_alive(), "worker 线程未在超时内退出")
        return wr, out

    def _assert_no_rows(self, attempt_id):
        """失败方不得留下 attempt/deliverable/audit 记录。"""
        c = self.conn
        n_attempt = c.execute(
            "SELECT COUNT(*) FROM attempts WHERE id=?", (attempt_id,)
        ).fetchone()[0]
        self.assertEqual(n_attempt, 0, f"失败方不得产生 attempt：{attempt_id}")
        n_del = c.execute(
            "SELECT COUNT(*) FROM deliverables WHERE attempt_id=?", (attempt_id,)
        ).fetchone()[0]
        self.assertEqual(n_del, 0, f"失败方不得产生 deliverables：{attempt_id}")
        n_aud = c.execute(
            "SELECT COUNT(*) FROM audit WHERE entity='attempt' AND entity_id=?",
            (attempt_id,),
        ).fetchone()[0]
        self.assertEqual(n_aud, 0, f"失败方不得产生 attempt 审计：{attempt_id}")
        # task.auto 审计以 detail.attempt 引用认领的 attempt id（不依赖 SQLite JSON 扩展）
        n_auto = 0
        for row in c.execute("SELECT detail FROM audit WHERE action='task.auto'").fetchall():
            detail = json_loads(row["detail"], {})
            if detail.get("attempt") == attempt_id:
                n_auto += 1
        self.assertEqual(n_auto, 0,
                         f"失败方不得产生 task.auto 审计：{attempt_id}")

    def assert_claim_rejected(self, wr, code, task_id, attempt_id,
                              task_status, current):
        """认领应被拒绝：指定错误码、无副作用、current 指针与任务状态不受影响。"""
        self.assertIn("error", wr, f"认领不应成功：{wr.get('attempt')}")
        self.assertIsInstance(wr["error"], LedgerError)
        self.assertEqual(wr["error"].code, code)
        self.assertTrue(wr.get("conn_usable"),
                        f"claim 失败后连接应仍可用：{wr.get('conn_usable_error')!r}")
        self._assert_no_rows(attempt_id)
        task = self.ledger.get_task(task_id)
        self.assertEqual(task["status"], task_status)
        self.assertEqual(task["current_attempt_id"], current)

    # ---- 用例 -----------------------------------------------------
    def test_two_tasks_same_scope_at_most_one_owner(self):
        """OpenSpec 1.1 场景：不同任务并发请求同一目录，至多一方成功。"""
        for kind, sub in (("identical", None), ("parent-child", "child")):
            with self.subTest(kind=kind):
                site = os.path.join(self.home, f"site-{kind}")
                os.makedirs(site, exist_ok=True)
                scope_b = os.path.join(site, sub) if sub else site
                t1 = self.ledger.create_task(project="p", title=f"{kind}-one")
                t2 = self.ledger.create_task(project="p", title=f"{kind}-two")

                def compete(out, site=site):
                    out["winner"] = self.ledger.create_attempt(
                        t1["id"], directory=site, write_scope=[site],
                        attempt_id=f"A-winner-{kind}", status="running",
                        dispatch_state="confirmed")

                loser_id = f"A-loser-{kind}"
                wr, out = self.run_race(
                    t2["id"], loser_id, compete,
                    directory=scope_b, write_scope=[scope_b], status="running",
                    dispatch_state="confirmed", deliverables=["out.txt"],
                )
                self.assert_claim_rejected(wr, "SITE_BUSY", task_id=t2["id"],
                                           attempt_id=loser_id,
                                           task_status="draft", current=None)
                self.assertEqual(count_overlap_holders(self.conn, [site]), 1,
                                 "同 scope 不得同时存在两个未释放持有者")
                self.assertEqual(
                    self.ledger.get_task(t1["id"])["current_attempt_id"],
                    out["winner"]["id"])

    def test_same_task_different_scope_generation_auto_increments(self):
        """OpenSpec 2.1：同任务不同 scope 并发，后者自动递增 generation，不 IntegrityError。

        时钟回归（审查发现）：ts/meta 时间戳必须在事务内、守卫通过后生成。若 ts 在
        事务前采样，则 A 先取时间再等待、B 先提交 g2、A 获锁分配 g3 但 created_at
        反而早于 g2；attempts_for_session 按 created_at DESC 会让
        latest_attempt_for_session 指向旧代次 g2，与 current_attempt_id 矛盾。
        """
        site = os.path.join(self.home, "site")
        scope_a = os.path.join(site, "a")
        scope_b = os.path.join(site, "b")
        os.makedirs(scope_a)
        os.makedirs(scope_b)
        session = "ses-shared"
        clock = _FakeClock()
        with mock.patch("oc_orch.ledger.now_ms", new=clock):
            t = self.ledger.create_task(project="p", title="gen")
            a1 = self.ledger.create_attempt(
                t["id"], directory=scope_a, write_scope=[scope_a], status="running",
                dispatch_state="confirmed")
            self.ledger.release_site(a1["id"], confirmed=True, external_stopped=True,
                                     actor="test")
            loser_id = "A-gen3"

            def compete(out):
                clock.advance()                  # B 提交前推进时钟
                out["main"] = self.ledger.create_attempt(
                    t["id"], directory=scope_a, write_scope=[scope_a],
                    attempt_id="A-gen2-main", session_id=session, status="running",
                    dispatch_state="confirmed")
                clock.advance()                  # 释放 A 前再推进

            wr, out = self.run_race(
                t["id"], loser_id, compete,
                directory=scope_b, write_scope=[scope_b], session_id=session,
                status="running", dispatch_state="confirmed",
            )
            err = wr.get("error")
            self.assertIsNone(err, f"并发认领不得以异常崩溃：{err!r}")
            self.assertIn("attempt", wr, "自动 generation 递增后应成功创建")
            g3, g2 = wr["attempt"], out["main"]
            self.assertEqual(g3["generation"], g2["generation"] + 1)
            self.assertEqual(self.ledger.get_task(t["id"])["current_attempt_id"],
                             loser_id)
            # 时间戳顺序不变量：更高 generation 的 created_at 不得早于已提交代次
            self.assertGreaterEqual(
                g3["created_at"], g2["created_at"],
                "高代次 created_at 必须>=已提交代次：ts 须在事务内/守卫通过后取",
            )
            gens = [r["generation"] for r in self.conn.execute(
                "SELECT generation FROM attempts WHERE task_id=? ORDER BY generation",
                (t["id"],)).fetchall()]
            self.assertEqual(gens, [1, 2, 3])
            self.assertTrue(wr.get("conn_usable"))
            # attempts_for_session 按 created_at DESC：共享 session 的最新尝试须为 g3
            latest = self.ledger.latest_attempt_for_session(session)
            self.assertEqual(latest["id"], loser_id,
                             "同一 session 的最新尝试必须是最新 generation（g3）")

    def test_cancel_during_window_blocks_claim(self):
        """OpenSpec 2.1/1.1：事务等待期间任务被取消，认领报 TASK_TERMINAL。"""
        site = os.path.join(self.home, "site")
        os.makedirs(site, exist_ok=True)
        t = self.ledger.create_task(project="p", title="cancel")
        a1 = self.ledger.create_attempt(
            t["id"], directory=site, write_scope=[site], status="running",
            dispatch_state="confirmed")
        self.ledger.release_site(a1["id"], confirmed=True, external_stopped=True,
                                 actor="test")
        loser_id = "A-cancel-loser"
        wr, out = self.run_race(
            t["id"], loser_id,
            lambda out: out.setdefault("closed", self.ledger.close_task(
                t["id"], reason="user-cancel")),
            directory=site, write_scope=[site], status="running",
            dispatch_state="confirmed", deliverables=["out.txt"],
        )
        self.assert_claim_rejected(wr, "TASK_TERMINAL", task_id=t["id"],
                                   attempt_id=loser_id,
                                   task_status="cancelled", current=a1["id"])

    def test_stop_requested_unreleased_blocks_during_window(self):
        """OpenSpec 1.1：旧尝试仅提交停止请求、未确认释放时，新 claim 被 SITE_BUSY 挡住。"""
        site = os.path.join(self.home, "site")
        os.makedirs(site, exist_ok=True)
        t1 = self.ledger.create_task(project="p", title="holder")
        a1 = self.ledger.create_attempt(
            t1["id"], directory=site, write_scope=[site], status="running",
            dispatch_state="confirmed")
        self.ledger.release_site(a1["id"], confirmed=True, external_stopped=True,
                                 actor="test")
        t2 = self.ledger.create_task(project="p", title="blocked-claim")

        def compete(out):
            holder = self.ledger.create_attempt(
                t1["id"], directory=site, write_scope=[site],
                attempt_id="A-stop-holder", status="running",
                dispatch_state="confirmed")
            out["holder"] = self.ledger.stop_attempt(holder["id"], actor="test")

        loser_id = "A-stop-loser"
        wr, out = self.run_race(
            t2["id"], loser_id, compete,
            directory=site, write_scope=[site], status="dispatched",
            dispatch_state="confirmed", deliverables=["out.txt"],
        )
        self.assert_claim_rejected(wr, "SITE_BUSY", task_id=t2["id"],
                                   attempt_id=loser_id,
                                   task_status="draft", current=None)
        holder = self.ledger.get_attempt(out["holder"]["id"])
        self.assertEqual(holder["status"], "stop_requested")
        self.assertIsNone(holder["site_released_at"])
        self.assertIsNone(holder["stop_confirmed_at"])

    def test_failed_claim_rolls_back_and_connection_stays_usable(self):
        """失败认领必须无副作用，且连接在回滚后仍可继续真实写入。

        兼容性锚点：显式 generation 重复仍为 IntegrityError（不许被修复放宽）。
        """
        site = os.path.join(self.home, "site")
        os.makedirs(site, exist_ok=True)
        scope_b = os.path.join(self.home, "scope-b")
        os.makedirs(scope_b, exist_ok=True)

        t1 = self.ledger.create_task(project="p", title="holder")
        a1 = self.ledger.create_attempt(
            t1["id"], directory=site, write_scope=[site], status="running",
            dispatch_state="confirmed")
        t2 = self.ledger.create_task(project="p", title="loser")
        loser = "A-seq-loser"
        with self.assertRaises(LedgerError) as ctx:
            self.ledger.create_attempt(
                t2["id"], directory=site, write_scope=[site], attempt_id=loser,
                status="running", dispatch_state="confirmed",
                deliverables=["out.txt"])
        self.assertEqual(ctx.exception.code, "SITE_BUSY")
        self._assert_no_rows(loser)
        t2b = self.ledger.get_task(t2["id"])
        self.assertEqual(t2b["status"], "draft")
        self.assertIsNone(t2b["current_attempt_id"])
        ok = self.ledger.create_attempt(
            t2["id"], directory=scope_b, write_scope=[scope_b], status="running",
            dispatch_state="confirmed")
        self.assertEqual(ok["task_id"], t2["id"])

        self.ledger.release_site(ok["id"], confirmed=True, external_stopped=True,
                                 actor="test")
        self.ledger.release_site(a1["id"], confirmed=True, external_stopped=True,
                                 actor="test")
        t3 = self.ledger.create_task(project="p", title="gen-dup")
        self.ledger.create_attempt(
            t3["id"], directory=site, write_scope=[site], generation=1,
            status="running", dispatch_state="confirmed")
        dup = "A-gen-dup"
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.create_attempt(
                t3["id"], directory=scope_b, write_scope=[scope_b], generation=1,
                attempt_id=dup, status="running", dispatch_state="confirmed")
        self._assert_no_rows(dup)
        self.ledger.set_meta("f1.after.integrity", "ok")
        b2 = self.ledger.create_attempt(
            t3["id"], directory=scope_b, write_scope=[scope_b], generation=2,
            status="running", dispatch_state="confirmed")
        self.assertEqual(b2["generation"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
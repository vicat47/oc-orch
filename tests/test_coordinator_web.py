#!/usr/bin/env python3
"""coordinator-first 响应式 Web 契约测试。

覆盖（真实 loopback HTTP + 真实 SQLite 台账）：
- 稳定排序：可靠创建时间从旧到新、session ID 兜底；未知/缺失时间排后；元数据
  失败不使自动刷新重排（服务端 TTL 缓存 + 客户端合并，客户端合并由 node 实测）。
- 归属覆盖：显式任务绑定优先于项目租约；worker session 不自动成为调度者；
  无租约/停用/无当前任务的已登记调度者仍可见。
- 未绑定任务、operator 责任与无归属告警仍可见（入口数据）。
- 全量计数：任务超过快照 200 项时，归属与计数仍基于全量台账。
- 深链与返回：/api/coordinator/<sid>、/api/task/<id>、/api/attempt/<id> 与
  缺失对象 404 区分；页面含 hash 路由与 viewport 元数据。
- read_only 与写守卫：GET 可用、POST 被拒；危险操作确认仍在服务端。
- 静态响应式/可访问性断言；并尝试用已安装的 node 真实执行页面 JS 验证合并排序。

无法进行真实浏览器渲染（本机 Firefox headless 的 SWGL 渲染在此环境失效），
故响应式只做真实 HTTP + 静态结构 + node 执行 JS 的验证，并在文档中明示限制。
"""

from __future__ import annotations

import glob
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from contextlib import contextmanager
from unittest.mock import patch

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from oc_orch.db import init  # noqa: E402
from oc_orch.ledger import Ledger  # noqa: E402
from oc_orch.util import now_ms  # noqa: E402
from oc_orch.web import WebServer  # noqa: E402


class _MetaClient:
    """注入 session 元数据（标题/创建时间/在线）；可配置失败与计数。"""

    def __init__(self, sessions=None, *, fail_get=False, fail_active=False):
        raw = sessions or {}
        if isinstance(raw, dict):
            self.sessions = dict(raw)
        else:
            self.sessions = {s["id"]: s for s in raw}
        self.fail_get = fail_get
        self.fail_active = fail_active
        self.get_calls = 0
        self.active_calls = 0

    def active_sessions(self):
        self.active_calls += 1
        if self.fail_active:
            raise OSError("active-unknown")
        return {sid: {"type": "running"} for sid, s in self.sessions.items() if s.get("active")}

    def get_session(self, session_id):
        self.get_calls += 1
        if self.fail_get:
            raise OSError("meta-unknown")
        return self.sessions.get(session_id)


class CoordinatorWebBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="coord-web-", dir="/tmp/opencode")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        self.conn = init(self.tmp)
        self.ledger = Ledger(self.conn, self.tmp)

    def tearDown(self):
        self.server.stop() if getattr(self, "server", None) else None
        self.conn.close()

    def _start(self, client=None, *, read_only=False):
        self.server = WebServer(self.tmp, port=0, read_only=read_only)
        if client is not None:
            self.server.client_factory = lambda: client
        self.port = self.server.start()
        return self.port

    def req(self, path, method="GET", body=None, headers=None):
        data = None
        h = dict(headers or {})
        if body is not None:
            data = json.dumps(body).encode()
            h.setdefault("Content-Type", "application/json")
        r = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (self.port, path),
            data=data, headers=h, method=method)
        try:
            with urllib.request.urlopen(r, timeout=15) as resp:
                return resp.status, resp.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    def _coords(self):
        st, body = self.req("/api/coordinators")
        self.assertEqual(st, 200, body)
        return json.loads(body)


class TestOverviewStableOrdering(CoordinatorWebBase):
    def _seed_sessions(self):
        # 三个调度者：显式绑定到任务（任何状态都算），创建时间不同
        for sid, created, title in (
                ("ses_new", 9_000_000_000, "新调度者"),
                ("ses_middle", 5_000_000_000, "中间调度者"),
                ("ses_old", 1_000_000_000, "旧调度者")):
            self.ledger.create_task(
                project=f"p-{sid}", title=title, coordinator_session=sid)
        return _MetaClient([{"id": "ses_old", "title": "旧调度者",
                             "time": {"created": 1_000_000_000}, "active": True},
                            {"id": "ses_middle", "title": "中间调度者",
                             "time": {"created": 5_000_000_000}, "active": True},
                            {"id": "ses_new", "title": "新调度者",
                             "time": {"created": 9_000_000_000}, "active": False},
                            {"id": "ses_extra", "title": "只有元数据无台账对象",
                             "time": {"created": 3_000_000_000}, "active": False}])

    def test_known_created_sorted_oldest_first_and_no_extra(self):
        client = self._seed_sessions()
        self._start(client)
        data = self._coords()
        ids = [c["session"] for c in data["coordinators"]]
        # ses_extra 只有元数据却没有台账关联 → 不自动成为调度者
        self.assertNotIn("ses_extra", ids)
        self.assertEqual(ids, ["ses_old", "ses_middle", "ses_new"])
        self.assertEqual(data["meta"]["created_at_known"], 3)
        self.assertEqual(data["meta"]["created_at_unknown"], 0)

    def test_refresh_within_ttl_does_not_query_meta_again(self):
        client = self._seed_sessions()
        self._start(client)
        data1 = self._coords()
        data2 = self._coords()
        self.assertEqual([c["session"] for c in data1["coordinators"]],
                         [c["session"] for c in data2["coordinators"]])
        # TTL 缓存：第二次 /api/coordinators 不逐次查询远端（排除 boot 外的刷新）
        self.assertEqual(client.get_calls, 3)  # 只有三张卡一次取数
        self.assertEqual(client.active_calls, 1)


    def test_session_run_state_tracks_active_snapshot_before_metadata_ttl(self):
        client = self._seed_sessions()
        self._start(client)
        def states():
            return {c["session"]: c["session_run_state"] for c in self._coords()["coordinators"]}

        with patch("oc_orch.entrypoints.web.monotonic") as clock:
            clock.return_value = 100
            self.assertEqual(states(), {"ses_old": "running", "ses_middle": "running", "ses_new": "inactive"})
            client.sessions["ses_old"]["active"] = False
            client.sessions["ses_new"]["active"] = True
            clock.return_value += 5
            self.assertEqual(states(), {"ses_old": "inactive", "ses_middle": "running", "ses_new": "running"})
            client.fail_active = True
            clock.return_value += 5
            self.assertEqual(set(states().values()), {"unknown"})
            client.fail_active = False
            clock.return_value += 5
            with patch.object(client, "active_sessions", return_value={"ses_new": {"type": "other"}}):
                self.assertEqual(states()["ses_new"], "unknown")
            clock.return_value += 5
            self.assertEqual(states()["ses_new"], "running")
        self.assertEqual(client.get_calls, 3)


class TestOrderingFallbacks(CoordinatorWebBase):
    def _seed(self):
        for sid, created in (("ses_known_a", 2_000_000_000),
                             ("ses_known_z", 6_000_000_000),
                             ("ses_no_time_b", None),
                             ("ses_no_time_a", None)):
            self.ledger.create_task(project=f"p-{sid}", title="t",
                                    coordinator_session=sid)
        sessions = [{"id": "ses_known_a", "title": "a",
                     "time": {"created": 2_000_000_000}, "active": True},
                    {"id": "ses_known_z", "title": "z",
                     "time": {"created": 6_000_000_000}, "active": True}]
        # 两个 session 的 time 缺失/为空
        sessions.append({"id": "ses_no_time_b", "title": "b", "time": {}, "active": False})
        sessions.append({"id": "ses_no_time_a", "title": "aa", "active": False})  # 完全无 time
        return _MetaClient(sessions)

    def test_unknown_created_sorted_by_session_id_after_known(self):
        self._start(self._seed())
        data = self._coords()
        ids = [c["session"] for c in data["coordinators"]]
        # 已知时间从旧到新排前；未知（无 time.created / 缺失）排后按 session ID
        self.assertEqual(ids[:2], ["ses_known_a", "ses_known_z"])
        self.assertEqual(ids[2:], ["ses_no_time_a", "ses_no_time_b"])
        for c in data["coordinators"]:
            if c["session"].startswith("ses_no_time"):
                self.assertFalse(c["created_at_known"])
                self.assertIsNone(c["created_at"])
        self.assertEqual(data["meta"]["created_at_known"], 2)
        self.assertEqual(data["meta"]["created_at_unknown"], 2)


class TestMetaFailureStability(CoordinatorWebBase):
    def test_failure_keeps_stable_fallback_and_marks_unknown(self):
        for sid in ("ses_f1", "ses_f2"):
            self.ledger.create_task(project=f"p-{sid}", title="t",
                                    coordinator_session=sid)
        failing = _MetaClient({"ses_f1": {"id": "ses_f1"}, "ses_f2": {"id": "ses_f2"}},
                              fail_get=True)
        self._start(failing)
        data1 = self._coords()
        ids1 = [c["session"] for c in data1["coordinators"]]
        self.assertEqual(ids1, sorted(ids1), "失败时按 session ID 稳定兜底")
        for c in data1["coordinators"]:
            self.assertFalse(c["created_at_known"])
            self.assertTrue(c["created_meta_error"])
        # 再次请求（TTL 内）不回源、顺序不变
        data2 = self._coords()
        self.assertEqual([c["session"] for c in data2["coordinators"]], ids1)
        # 每个待取 session 各一次 get_session + 一次 active 快照；TTL 内不再回源
        self.assertEqual(failing.get_calls, 2)
        self.assertEqual(failing.active_calls, 1)
        # 单点失败时：整体 non-fresh；每入口 online 归未知（不把缓存 active 当最新事实）
        for c in data1["coordinators"]:
            self.assertFalse(c["metadata_fresh"])
            self.assertIsNone(c["online"])
        self.assertFalse(data1["meta"]["metadata_fresh"])
        # 过期后元数据补齐：已知时间生效，排序变为创建时间；先前相对顺序保持
        with patch("oc_orch.entrypoints.web.COORD_META_TTL_MS", 0), \
                patch("oc_orch.entrypoints.web.COORD_META_STALE_MS", 0):
            good = _MetaClient([{"id": "ses_f1", "time": {"created": 9_000_000_000}},
                                {"id": "ses_f2", "time": {"created": 1_000_000_000}}],
                               )
            self.server.client_factory = lambda: good
            data3 = self._coords()
            ids3 = [c["session"] for c in data3["coordinators"]]
        self.assertEqual(ids3, ["ses_f2", "ses_f1"])
        self.assertTrue(data3["meta"]["metadata_fresh"])
        self.assertTrue(all(c["metadata_fresh"] for c in data3["coordinators"]))


class TestOwnershipSemantics(CoordinatorWebBase):
    def test_explicit_binding_overrides_lease_and_worker_not_coordinator(self):
        lease_sid, explicit_sid, worker_sid = "ses_lease", "ses_explicit", "ses_worker"
        projects = "/tmp/opencode/coord-proj", "/tmp/opencode/coord-proj-2"
        # 项目租约持有者
        self.ledger.claim_coordinator(project=projects[0], session_id=lease_sid, actor="t")
        self.ledger.claim_coordinator(project=projects[1], session_id=lease_sid, actor="t")
        # T1：无显式绑定 → 按租约归 lease_sid
        t1 = self.ledger.create_task(project=projects[0], title="租约任务")
        # T2/T3：显式绑定到 explicit_sid（同项目 P0，覆盖租约）
        running = self.ledger.create_task(project=projects[0], title="显式任务1",
                                          coordinator_session=explicit_sid)
        self.ledger.transition_task(running["id"], "running")
        self.ledger.create_task(project=projects[1], title="显式任务2",
                                coordinator_session=explicit_sid)
        # worker session 只出现在 attempt 上，不是调度者
        # （当前树派单权威契约：attempt 的 subject 冻结为租约 holder=lease_sid，
        #  执行 session=worker_sid 仅作为 worker 身份记录在 attempt 上）
        self.ledger.create_attempt(t1["id"], directory=self.tmp,
                                   session_id=worker_sid, coordinator=lease_sid)

        client = _MetaClient([{"id": lease_sid, "time": {"created": 100}},
                              {"id": explicit_sid, "time": {"created": 200}},
                              {"id": worker_sid, "time": {"created": 300}}])

        def factory():
            return client
        self._start(client)
        data = self._coords()
        ids = [c["session"] for c in data["coordinators"]]
        self.assertIn(lease_sid, ids)
        self.assertIn(explicit_sid, ids)
        self.assertNotIn(worker_sid, ids)
        by_id = {c["session"]: c for c in data["coordinators"]}
        exp = by_id[explicit_sid]
        # explicit 拥有 2 个未终态任务（显式绑定优先于租约）
        self.assertEqual(exp["counts"]["unfinished"], 2)
        self.assertEqual(exp["counts"]["accepted"], 0)
        self.assertEqual(exp["counts"]["running"], 1)
        self.assertEqual(exp["session_run_state"], "inactive")
        lease = by_id[lease_sid]
        self.assertEqual(lease["counts"]["unfinished"], 1)
        self.assertEqual(lease["counts"]["running"], 0)

        # 工作台详情：owned_via 区分
        st, body = self.req("/api/coordinator/" + explicit_sid)
        self.assertEqual(st, 200, body)
        detail = json.loads(body)
        self.assertEqual(detail["coordinator"]["session_run_state"], "inactive")
        self.assertEqual(len(detail["tasks"]), 2)
        self.assertTrue(all(t["owned_via"] == "explicit" for t in detail["tasks"]))

    def test_coordinator_without_current_tasks_still_visible(self):
        # 只有租约、没有任务的已登记调度者仍可见（停用/无当前任务不隐藏）
        self.ledger.claim_coordinator(project="/p-only-lease", session_id="ses_retired",
                                      actor="t")
        self._start(_MetaClient([{"id": "ses_retired", "title": "已停用",
                                  "time": {"created": 500}}]))
        data = self._coords()
        ids = [c["session"] for c in data["coordinators"]]
        self.assertIn("ses_retired", ids)
        c = next(x for x in data["coordinators"] if x["session"] == "ses_retired")
        self.assertEqual(c["counts"]["unfinished"], 0)
        self.assertEqual(c["attention_rank"], 6)  # 无待办关注，仍列出


class TestUnassignedAndGlobalVisibility(CoordinatorWebBase):
    def test_unassigned_tasks_visible_in_overview_meta(self):
        self.ledger.claim_coordinator(project="/p-lease", session_id="ses_one", actor="t")
        self.ledger.create_task(project="/p-lease", title="租约归属")
        self.ledger.create_task(project="/p-no-lease", title="未绑定任务")
        self._start(_MetaClient([{"id": "ses_one", "time": {"created": 100}}]))
        data = self._coords()
        self.assertEqual(data["meta"]["unbound_task_count"], 1)
        self.assertEqual(data["meta"]["unbound_tasks"][0]["title"], "未绑定任务")

    def test_operator_responsibility_and_unattributable_alert_visible(self):
        t = self.ledger.create_task(project="/p-op", title="op 责任")
        self.ledger.add_alert(severity="warning", kind="global_fault",
                              title="全局故障", detail="无归属",
                              dedupe_key="global:1")
        class _FakeResp:
            def list_open_responsibilities(self, **kw):
                return [{"id": "R-op1", "state": "awaiting_review",
                         "task_id": t["id"], "attempt_id": None, "generation": 1,
                         "result_version": None, "owner": "operator",
                         "target_session": None, "wait_kind": "human",
                         "wait_condition": "operator must decide",
                         "next_action": "人工接管", "next_check_at": now_ms(),
                         "deadline": now_ms() + 3600_000, "budget_used": 0,
                         "max_budget": 3, "revision": 1}]

        @contextmanager
        def opened(home):
            yield _FakeResp()
        with patch("oc_orch.entrypoints.web._open_responsibilities", opened):
            self._start(_MetaClient({}))
            # operator 责任可经 /api/responsibilities 读取
            st, body = self.req("/api/responsibilities?limit=10")
            data = json.loads(body)
            self.assertEqual(st, 200, body)
            self.assertEqual(data["responsibilities"][0]["owner"], "operator")
            # 无归属告警在快照中可见
            st, body = self.req("/api/status")
            snap = json.loads(body)["snapshot"]
            names = [a["kind"] for a in snap["alerts"]]
            self.assertIn("global_fault", names)
            al = next(a for a in snap["alerts"] if a["kind"] == "global_fault")
            self.assertIsNone(al.get("task_id"))
            self.assertIsNone(al.get("attempt_id"))

    def test_counts_beyond_snapshot_limit_are_full(self):
        # 任务 > 200：总览归属计数必须基于全量台账，不能把快照 200 项当全部
        client = _MetaClient([{"id": "ses_bulk", "time": {"created": 50}}])
        for i in range(215):
            self.ledger.create_task(project="/p-bulk", title="任务 %d" % i,
                                    coordinator_session="ses_bulk")
        self._start(client)
        data = self._coords()
        c = next(x for x in data["coordinators"] if x["session"] == "ses_bulk")
        self.assertEqual(c["counts"]["unfinished"], 215)
        self.assertTrue(c["task_tips_truncated"])
        self.assertTrue(data["meta"]["task_counts_note"])
        # 快照本身仍是 200 上限：避免把限量当全量
        st, body = self.req("/api/status")
        snap = json.loads(body)["snapshot"]
        self.assertEqual(len(snap["tasks"]), 200)


class TestDeepLinks(CoordinatorWebBase):
    def test_task_and_attempt_deep_links_and_missing(self):
        client = _MetaClient({})
        t = self.ledger.create_task(project="/p", title="深链任务",
                                    coordinator_session="ses_dl")
        a = self.ledger.create_attempt(t["id"], directory=self.tmp,
                                       session_id="ses_worker")
        self._start(client)
        st, body = self.req("/api/task/" + t["id"])
        self.assertEqual(st, 200, body)
        self.assertEqual(json.loads(body)["task"]["id"], t["id"])
        st, body = self.req("/api/attempt/" + a["id"])
        self.assertEqual(st, 200, body)
        data = json.loads(body)
        self.assertEqual(data["attempt"]["id"], a["id"])
        self.assertEqual(data["task"]["id"], t["id"])
        # attempt_detail 收窄：不返回任意 metadata / 写范围 / 页面未用字段
        att = data["attempt"]
        for unwanted in ("metadata", "write_scope", "agent", "model", "variant"):
            self.assertNotIn(unwanted, att, unwanted)
        for wanted in ("verification", "result_path", "result_summary", "deliverables"):
            self.assertIn(wanted, data if wanted == "deliverables" else att, wanted)
        for path, code in (("/api/task/T-none", "TASK_NOT_FOUND"),
                           ("/api/attempt/A-none", "ATTEMPT_NOT_FOUND"),
                           ("/api/coordinator/ses_gone", "COORDINATOR_NOT_FOUND")):
            st, body = self.req(path)
            self.assertEqual(st, 404, body)
            self.assertEqual(json.loads(body)["code"], code)

    def test_page_has_routes_and_viewport(self):
        self._start(_MetaClient({}))
        st, body = self.req("/")
        self.assertEqual(st, 200)
        st, js = self.req("/assets/app.js")
        self.assertEqual(st, 200, js)
        # 页面外壳：viewport 与本地同源资源引用，无 CDN/内联脚本
        for needle in ('name="viewport"', "/assets/app.js", "/assets/app.css",
                       "oc-orch 控制面"):
            self.assertIn(needle, body, needle)
        self.assertNotIn('src="http', body, "页面不得引用外部脚本")
        self.assertNotIn('href="http', body, "页面不得引用外部样式")
        # 打包 JS：hash 路由（含各视图分支）、hashchange 监听与单飞刷新导出
        for needle in ("hashchange", "refreshLight", "coordinator", "tasks",
                       "diagnostics", "routeKey"):
            self.assertIn(needle, js, needle)


class TestReadOnlyAndSafety(CoordinatorWebBase):
    def test_read_only_get_ok_write_blocked(self):
        self.ledger.claim_coordinator(project="/p-ro", session_id="ses_ro", actor="t")
        self._start(_MetaClient([{"id": "ses_ro", "time": {"created": 5}}]),
                    read_only=True)
        st, body = self.req("/api/coordinators")
        self.assertEqual(st, 200, body)
        st, body = self.req("/api/coordinator/unlock", method="POST",
                            body={"project": "/p-ro", "session": "ses_ro",
                                  "reason": "web", "confirm": "UNLOCK"},
                            headers={"X-OC-Orch-Web": "1"})
        self.assertEqual(st, 403, body)
        self.assertEqual(json.loads(body)["code"], "READ_ONLY")

    def test_dangerous_action_requires_confirm(self):
        t = self.ledger.create_task(project="/p-c", title="确认")
        self._start(_MetaClient({}))
        st, body = self.req(
            f"/api/task/{t['id']}/cancel", method="POST",
            body={"reason": "web"}, headers={"X-OC-Orch-Web": "1"})
        self.assertEqual(st, 400, body)
        self.assertEqual(self.ledger.get_task(t["id"])["status"], "draft")


class TestResponsiveStaticAssertions(CoordinatorWebBase):
    def _assets(self):
        self._start(_MetaClient({}))
        st, html = self.req("/")
        self.assertEqual(st, 200, html)
        st, css = self.req("/assets/app.css")
        self.assertEqual(st, 200, css)
        st, js = self.req("/assets/app.js")
        self.assertEqual(st, 200, js)
        return html, css, js

    def test_viewport_media_touch_and_no_global_overflow(self):
        html, css, js = self._assets()
        combo = "\n".join((html, re.sub(r"\s*([:{}])\s*", r"\1", css), js))
        for needle in (
                'name="viewport" content="width=device-width, initial-scale=1',
                "@media (max-width:768px)",
                "min-height:var(--touch)",
                "--touch:44px",
                ".tablewrap{",
                ":focus-visible{",
                "body{overflow-x:hidden}",
                "overflow-wrap:anywhere",
                "aria-live=",
                'role="tablist"',
                "prefers-reduced-motion",
        ):
            self.assertIn(needle, combo, needle)
        # 触控目标在窄屏不小于 44px（CSS 变量 + media；CSS 从真实资源断言）
        self.assertGreaterEqual(int(css.split("--touch:")[1].split("px")[0]), 44)
        # 关键交互不依赖 hover：卡片入口是 hash 链接，复制是独立按钮（打包 JS 中）
        self.assertIn('#/coordinator/', js)  # Lit 模板字面量 ${`#/coordinator/...`}
        self.assertIn("data-copy=", js)

    def test_page_served(self):
        self._start(_MetaClient({}))
        st, body = self.req("/")
        self.assertEqual(st, 200)
        self.assertIn("oc-orch 控制面", body)
        self.assertIn("/assets/app.js", body, "页面外壳应引用本地同源 JS")


@unittest.skipUnless(shutil.which("node"), "未安装 node，跳过真实 DOM 验证")
class TestPageJsBehavior(unittest.TestCase):
    """真实 DOM（jsdom）应用行为测试：由 Python 触发 npm test 执行 web/tests/。

    迁移说明：旧实现用 vm 最小桩执行内联 JS，只测“影子 DOM 桩”，无法验证真实
    渲染/事件/输入保全。现由 Node jsdom 测试直接驱动随 wheel/sdist 分发的打包
    产物 app.js（经 oc-orch-app light DOM），并保持 29 项旧语义等价：
      - 合并保序/刷新保留视图 → keyed 更新节点身份保留 + 无加载占位
      - 编辑/聚焦时不重绘 → 表单值/焦点/caret、checkbox、details、搜索焦点保全
      - extras 错误标记复位  → 失败保留旧数据并标过期、恢复清除
      - 全局 data-act 委托（unlock/acknote）→ repeat 刷新后一次点击仅一次 POST
      - notify-only 身份标注 → read_only/确认/XSS 安全性
    仅当 node + web/node_modules/jsdom + 打包产物齐备时运行；真实浏览器验证为
    可选项（web/tests/browser，chromium 局部缓存），本机 Firefox SWGL 失效不冒充。
    """
    WEB_DIR = os.path.join(os.path.dirname(HERE), "web")

    def test_node_jsdom_behavior_suite(self):
        if not os.path.exists(os.path.join(self.WEB_DIR, "node_modules", "jsdom")):
            self.skipTest("web/node_modules 未安装 jsdom（npm ci --ignore-scripts 后重试）")
        files = sorted(glob.glob(os.path.join(self.WEB_DIR, "tests", "*.test.mjs")))
        self.assertTrue(files, "web/tests/*.test.mjs 缺失，真实 DOM 测试应随源码提交")
        env = os.environ.copy()
        env.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/tmp/opencode/pw-browsers")
        proc = subprocess.run(
            ["node", "--test", *files],
            cwd=self.WEB_DIR, env=env, capture_output=True, text=True, timeout=300)
        last = (proc.stdout + "\n--- stderr ---\n" + proc.stderr)[-6000:]
        self.assertEqual(proc.returncode, 0,
                         "node jsdom 行为测试失败:\n%s" % last)
        m = re.search(r"(?:#|ℹ)\s*pass\s+(\d+)", proc.stdout)
        self.assertTrue(m, "无法从测试输出解析通过计数:\n%s" % last)
        self.assertGreaterEqual(int(m.group(1)), 1,
                                "至少一组真实 DOM 测试应通过（避免零测试假绿）:\n%s" % last)

class TestMetadataFreshnessPerEntry(CoordinatorWebBase):
    """单个 session 元数据查询失败：该入口非 fresh、整体非 fresh，online 归未知。"""

    def _seed(self):
        self.ledger.create_task(project="/p-a", title="a", coordinator_session="ses_ok")
        self.ledger.create_task(project="/p-b", title="b", coordinator_session="ses_bad")

        class _Mixed(_MetaClient):
            def get_session(self, sid):
                self.get_calls += 1
                if sid == "ses_bad":
                    raise OSError("meta-down")
                return {"id": sid, "time": {"created": 1000}, "active": True}

            def active_sessions(self):
                self.active_calls += 1
                return {"ses_ok": {"type": "running"}}

        return _Mixed({})

    def test_per_entry_freshness_and_overall(self):
        client = self._seed()
        self._start(client)
        data = self._coords()
        by = {c["session"]: c for c in data["coordinators"]}
        ok, bad = by["ses_ok"], by["ses_bad"]
        self.assertTrue(ok["metadata_fresh"])
        self.assertIs(ok["online"], True)
        self.assertEqual(ok["session_run_state"], "running")
        self.assertEqual(bad["session_run_state"], "unknown")
        self.assertFalse(bad["metadata_fresh"])
        self.assertTrue(bad["created_meta_error"])
        self.assertIsNone(bad["online"], "失败时不把缓存 active 当最新事实")
        self.assertFalse(data["meta"]["metadata_fresh"], "任一入口失败整体即非 fresh")


class TestUnboundTasksPagination(CoordinatorWebBase):
    def test_full_count_peek_and_keyset_paging(self):
        for i in range(25):
            self.ledger.create_task(project="/p-u", title="未绑定任务 %d" % i)
        self._start(_MetaClient({}))
        # 总览全量计数（不受快照 200 项限制、peek 有截断标注）
        data = self._coords()
        self.assertEqual(data["meta"]["unbound_task_count"], 25)
        self.assertTrue(data["meta"]["unbound_truncated"])
        self.assertEqual(len(data["meta"]["unbound_tasks"]), 20)
        # 分页：第一页 10
        st, body = self.req("/api/tasks/unbound?limit=10")
        self.assertEqual(st, 200, body)
        p1 = json.loads(body)
        self.assertEqual(len(p1["tasks"]), 10)
        self.assertEqual(p1["full_count"], 25)
        self.assertTrue(p1["truncated"])
        self.assertTrue(p1["next_after_id"])
        ids1 = [t["id"] for t in p1["tasks"]]
        self.assertEqual(ids1, sorted(ids1), "keyset 按 id 升序稳定")
        # 第二页
        st, body = self.req("/api/tasks/unbound?limit=10&after_id=" + p1["next_after_id"])
        p2 = json.loads(body)
        ids2 = [t["id"] for t in p2["tasks"]]
        self.assertEqual(len(ids2), 10)
        self.assertFalse(set(ids1) & set(ids2), "分页不重复")
        # 尾页
        st, body = self.req("/api/tasks/unbound?limit=10&after_id=" + p2["next_after_id"])
        p3 = json.loads(body)
        ids3 = [t["id"] for t in p3["tasks"]]
        self.assertEqual(len(ids3), 5)
        self.assertFalse(p3["truncated"])
        self.assertIsNone(p3["next_after_id"])
        self.assertEqual(len(set(ids1) | set(ids2) | set(ids3)), 25, "三页覆盖全部")


class TestCoordinatorWorkspacePagination(CoordinatorWebBase):
    def test_workspace_tasks_paged_by_id_with_continue(self):
        for i in range(5):
            self.ledger.create_task(project="/p-pg", title="任务 %d" % i,
                                    coordinator_session="ses_pg")
        self._start(_MetaClient([{"id": "ses_pg", "time": {"created": 5}}]))
        st, body = self.req("/api/coordinator/ses_pg?limit=2")
        self.assertEqual(st, 200, body)
        p1 = json.loads(body)
        self.assertEqual(len(p1["tasks"]), 2)
        self.assertTrue(p1["tasks_truncated"])
        self.assertEqual(p1["tasks_full_count"], 5)
        self.assertEqual(p1["unfinished_full_count"], 5)
        self.assertTrue(p1["next_after_id"])
        ids1 = [t["id"] for t in p1["tasks"]]
        self.assertEqual(ids1, sorted(ids1), "工作台任务按 ID 稳定升序")
        st, body = self.req("/api/coordinator/ses_pg?limit=2&after_id=" + p1["next_after_id"])
        p2 = json.loads(body)
        ids2 = [t["id"] for t in p2["tasks"]]
        self.assertEqual(len(ids2), 2)
        self.assertFalse(set(ids1) & set(ids2))
        st, body = self.req("/api/coordinator/ses_pg?limit=2&after_id=" + p2["next_after_id"])
        p3 = json.loads(body)
        self.assertEqual(len(p3["tasks"]), 1)
        self.assertFalse(p3["tasks_truncated"])
        self.assertIsNone(p3["next_after_id"])
        self.assertEqual(len(set(ids1) | set(ids2) | {p3["tasks"][0]["id"]}), 5)
        self.assertTrue(all(t["owned_via"] == "explicit" for t in p1["tasks"]))


class TestNotificationTargetCompleteness(CoordinatorWebBase):
    def test_targets_beyond_arbitrary_limit_still_discovered(self):
        # 直接登记 1003 条通知目标（含 acked 历史）——超过旧实现 1000 条任意 LIMIT
        ts = now_ms()
        n = 1003
        with self.conn:
            for i in range(n):
                sid = "ses_tgt_%04d" % i
                self.conn.execute(
                    "INSERT INTO notifications(id, kind, target_session, dedupe_key, "
                    "payload, state, priority, attempts, max_attempts, next_attempt_at, "
                    "created_at, metadata) VALUES(?,?,?,?,?,?,0,1,3,?,?,'{}')",
                    ("N-tgt-%04d" % i, "test", sid, "dup-%04d" % i, "{}", "acked",
                     ts, ts))
        self._start(_MetaClient({}))
        data = self._coords()
        ids = {c["session"] for c in data["coordinators"]}
        self.assertIn("ses_tgt_0000", ids)
        self.assertIn("ses_tgt_1002", ids, "超过旧 LIMIT 的历史目标不得遗漏")
        self.assertGreaterEqual(data["meta"]["session_count"], n)
        c = next(x for x in data["coordinators"] if x["session"] == "ses_tgt_1002")
        # acked 历史也是发现来源：sources 依据全量通知历史集合而非 pending 计数
        self.assertListEqual(c["sources"], ["notify:ses_tgt_1002"], "通知历史来源明确标注")
        self.assertEqual(c["counts"]["notifications_pending"], 0, "pending 计数独立于来源")
        self.assertEqual(c["identity"], "notify-associated", "acked 历史目标未确认调度者角色")
        self.assertIn("通知目标", data["meta"]["scope"])
        self.assertIn("关联会话", data["meta"]["scope"])

    def test_worker_notification_target_role_not_confirmed(self):
        # 纯 worker（仅被通知投递、无任务绑定/租约）：出现在发现集但身份未确认；
        # UI 依据 identity + sources 显示“调度者角色未确认”，不误称确认协调者。
        worker = "ses_worker_only_notify"
        self.ledger.enqueue_notification(
            kind="test", target_session=worker, dedupe_key="n:w1", payload={"summary": "x"})
        self._start(_MetaClient({}))
        data = self._coords()
        ids = {c["session"] for c in data["coordinators"]}
        self.assertIn(worker, ids)
        c = next(x for x in data["coordinators"] if x["session"] == worker)
        self.assertListEqual(c["sources"], ["notify:" + worker])
        self.assertEqual(c["identity"], "notify-associated", "worker 不得标为 confirmed")
        self.assertNotIn("lease:", c["sources"])
        self.assertNotIn("task:", c["sources"])
        self.assertEqual(c["counts"]["notifications_pending"], 1, "pending 计数仍独立展示")


class TestAttemptDetailNarrowing(CoordinatorWebBase):
    def test_attempt_detail_excludes_unused_arbitrary_metadata(self):
        t = self.ledger.create_task(project="/p-an", title="an")
        self.ledger.create_attempt(
            t["id"], directory=self.tmp, session_id="ses_w",
            write_scope=[self.tmp],
            metadata={"secret": "x", "arbitrary": [1, 2]})
        self._start(_MetaClient({}))
        a = self.ledger.list_attempts(task_id=t["id"])[0]
        st, body = self.req("/api/attempt/" + a["id"])
        self.assertEqual(st, 200, body)
        att = json.loads(body)["attempt"]
        self.assertNotIn("metadata", att)
        self.assertNotIn("write_scope", att)
        self.assertNotIn("secret", json.loads(body))
        self.assertIn("verification", att)
        self.assertIn("result_path", att)
        self.assertIn("result_summary", att)
        self.assertEqual(att["directory"], self.tmp)


class TestTaskEffectiveCoordinator(CoordinatorWebBase):
    def test_lease_fallback_explicit_and_unbound(self):
        holder = "ses_holder"
        self.ledger.claim_coordinator(project="/p-lease", session_id=holder, actor="t")
        explicit = "ses_exp"
        t_lease = self.ledger.create_task(project="/p-lease", title="租约归属任务")
        t_exp = self.ledger.create_task(project="/p-lease", title="显式任务",
                                        coordinator_session=explicit)
        t_none = self.ledger.create_task(project="/p-nolease", title="未绑定")
        self._start(_MetaClient({}))
        st, body = self.req("/api/task/" + t_lease["id"])
        self.assertEqual(st, 200, body)
        data = json.loads(body)
        self.assertEqual(data["task"]["effective_coordinator"], holder)
        self.assertEqual(data["task"]["coordinator_source"], "lease")
        st, body = self.req("/api/task/" + t_exp["id"])
        data = json.loads(body)
        self.assertEqual(data["task"]["effective_coordinator"], explicit)
        self.assertEqual(data["task"]["coordinator_source"], "explicit")
        st, body = self.req("/api/task/" + t_none["id"])
        data = json.loads(body)
        self.assertIsNone(data["task"]["effective_coordinator"])
        self.assertIsNone(data["task"]["coordinator_source"])


class TestUnboundEmptyString(CoordinatorWebBase):
    def test_empty_binding_with_or_without_lease(self):
        holder = "ses_holder"
        self.ledger.claim_coordinator(project="/p-le", session_id=holder, actor="t")
        self.ledger.create_task(project="/p-le", title="空串有租约", coordinator_session="")
        self.ledger.create_task(project="/p-no", title="空串无租约", coordinator_session="")
        self._start(_MetaClient({}))
        # 空串 coordinator_session 视同未绑定：有租约的项目归租约持有者，无租约计入未绑定
        st, body = self.req("/api/tasks/unbound?limit=50")
        self.assertEqual(st, 200, body)
        unbound = json.loads(body)
        self.assertEqual(unbound["full_count"], 1)
        self.assertEqual([t["title"] for t in unbound["tasks"]], ["空串无租约"])
        data = self._coords()
        self.assertEqual(data["meta"]["unbound_task_count"], 1)
        holder_item = next(x for x in data["coordinators"] if x["session"] == holder)
        self.assertEqual(holder_item["counts"]["unfinished"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)

#!/usr/bin/env python3
"""R2 Web/API 安全反例（独立验证）：类型严格/CAS·重放·多项目·分页不误认唯一项目。

只读审查范围：GET /api/health、/api/incidents、/api/coordinator/<sid>、
POST /api/coordinator/pause|resume（src/oc_orch/entrypoints/web.py）及
web/src 的 health/proj control/verifyDrafts（app.js/data.js/state.js/
actions.js/views/{coordinator,task,attempt,forms}.js）。

worker初10项确认通常项目名下的拒绝路径；父核收又补同名字面量已存在、
operation/reason类型及带空白confirm反例，暴露入口str转换/strip的实际缺口。
其余断言包括多项目全量归属、health按sid隔离及验收草稿按routeKey隔离。
前端反例使用真实jsdom，不以影子实现冒充产品行为。

只使用临时 SQLite + 真实 WebServer（fake client）+ web 已有 node 测试 harness
（jsdom）；零真实服务/邮件/配置/安装/Git；不改任何 src/web/原 tests/docs。
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
SRC = REPO / "src"
WEB = REPO / "web"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from oc_orch import composition  # noqa: E402
from oc_orch.ledger import Ledger  # noqa: E402
from oc_orch.web import WebServer  # noqa: E402


class _NoClient:
    """控制/只读路径不得惰性构造 Client；被构造即失败。"""

    def __init__(self):
        self.called = False

    def __call__(self):
        self.called = True
        raise AssertionError("pause/resume 不应构造远端 Client")


class SafetyWebBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="web-safety-", dir="/tmp/opencode")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        self.conn = composition.init(self.tmp)
        self.addCleanup(self.conn.close)
        self.ledger = Ledger(self.conn, self.tmp)
        self.lease_p = self.ledger.claim_coordinator(
            project="p", session_id="ses_coord", actor="test", ttl=3600)
        self.task_p = self.ledger.create_task(
            project="p", title="task-p", coordinator_session="ses_coord")
        self.client = _NoClient()
        self.server = WebServer(self.tmp, port=0, read_only=False)
        self.server.client_factory = self.client
        self.port = self.server.start()

    def tearDown(self):
        self.server.stop()

    def get(self, path):
        r = urllib.request.Request("http://127.0.0.1:%d%s" % (self.port, path))
        try:
            with urllib.request.urlopen(r, timeout=15) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def post(self, path, body):
        h = {"Content-Type": "application/json", "X-OC-Orch-Web": "1"}
        r = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (self.port, path),
            data=json.dumps(body).encode(), method="POST", headers=h)
        try:
            with urllib.request.urlopen(r, timeout=15) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def base_body(self, action, **overrides):
        body = {
            "project": "p", "session_id": "ses_coord",
            "operation_id": "web-%s" % action,
            "reason": "web 测试", "expected_revision": 0,
            "expected_epoch": self.lease_p["epoch"], "confirm": "p",
        }
        body.update(overrides)
        return body

    def control_meta_rows(self, prefix="coordinator-control:project:"):
        return self.conn.execute(
            "SELECT key FROM meta WHERE key LIKE ?", (prefix + "%",)).fetchall()

    def control_audit_count(self, action=None):
        sql = ("SELECT COUNT(*) FROM audit WHERE action LIKE 'coordinator-control.%'")
        args: tuple = ()
        if action is not None:
            sql += " AND action=?"
            args = (action,)
        return self.conn.execute(sql, args).fetchone()[0]

    def paused_intents(self):
        rows = self.conn.execute(
            "SELECT key, value FROM meta WHERE key LIKE 'coordinator-control:project:%'"
        ).fetchall()
        out = {}
        for key, value in rows:
            out[key] = "paused" if '"status": "paused"' in value else "running"
        return out


class PostTypeStrictnessCounterTests(SafetyWebBase):
    """数组/对象/布尔不能经 str 转换冒充真实 project/session/CAS。"""

    def test_malformed_project_cannot_alias_existing_string_literal_project(self):
        # 不能靠“转换后的项目碰巧不存在”证明类型安全：登记这个真实项目后，
        # 列表经str转换会误投同名字面量项目，并与同样转换的confirm相等。
        literal = "['p']"
        self.ledger.claim_coordinator(project=literal, session_id="ses_coord", actor="test", ttl=3600)
        self.ledger.create_task(project=literal, title="literal project", coordinator_session="ses_coord")
        status, data = self.post("/api/coordinator/pause", self.base_body(
            "pause", project=["p"], confirm=["p"]))
        self.assertEqual(status, 400, data)
        self.assertEqual(self.control_meta_rows(), [])
        self.assertEqual(self.control_audit_count(), 0)

    def test_operation_and_reason_are_real_strings_not_coerced_values(self):
        for field, value in (("operation_id", ["op"]), ("operation_id", {"id": "op"}),
                             ("reason", ["reason"]), ("reason", {"text": "reason"}),
                             ("reason", True)):
            with self.subTest(field=field, value=value):
                status, data = self.post("/api/coordinator/pause", self.base_body(
                    "pause", **{field: value}))
                self.assertEqual(status, 400, data)
        self.assertEqual(self.control_meta_rows(), [])
        self.assertEqual(self.control_audit_count(), 0)

    def test_confirm_whitespace_is_not_an_exact_project_confirmation(self):
        for confirm in (" p", "p ", "\tp\n"):
            with self.subTest(confirm=confirm):
                status, data = self.post("/api/coordinator/pause", self.base_body("pause", confirm=confirm))
                self.assertEqual(status, 400, data)
                self.assertEqual(data["code"], "CONFIRM_REQUIRED")
        self.assertEqual(self.control_meta_rows(), [])

    def test_project_whitespace_is_not_silently_routed_to_other_identity(self):
        status, data = self.post("/api/coordinator/pause", self.base_body(
            "pause", project=" p ", confirm=" p "))
        self.assertEqual(status, 400, data)
        self.assertEqual(self.control_meta_rows(), [])

    def test_array_project_and_confirm_str_equal_never_touches_real_project(self):
        # project 与 confirm 同为列表：str 转换后相等（'['p']' == '['p']'），
        # 若下游宽松会“冒认一致”并产生一次对不存在项目“['p']”的写入尝试。
        # 反例要求：400 拒绝，且真实项目 p 的控制与租约零变化、无任何控制写入。
        status, data = self.post("/api/coordinator/pause", self.base_body(
            "pause", project=["p"], confirm=["p"], session_id="ses_coord"))
        self.assertEqual(status, 400, data)
        self.assertEqual(self.control_meta_rows(), [], "不得伪造项目控制记录")
        self.assertEqual(self.control_audit_count(), 0)
        _, health = self.get("/api/health")
        item = next(t for t in health["tasks"] if t["task_id"] == self.task_p["id"])
        self.assertEqual(item["control"]["project_intent"], "running")
        self.assertEqual(self.paused_intents(), {})
        self.assertFalse(self.client.called)

    def test_list_or_object_session_and_project_rejected_without_write(self):
        cases = [
            # session 列表 → str "['ses_coord']"，holder 不匹配 → 400
            {"project": "p", "session_id": ["ses_coord"], "confirm": "p"},
            # session 对象 → str "{'sid': ...}"，holder 不匹配 → 400
            {"project": "p", "session_id": {"sid": "ses_coord"}, "confirm": "p"},
            # project 对象 → str "{'name': 'p'}" ≠ confirm "p" → 400
            {"project": {"name": "p"}, "confirm": "p"},
            # project 数字 → "1" 目标不存在 → lease-missing → 400
            {"project": 1, "confirm": "1"},
        ]
        for overrides in cases:
            with self.subTest(overrides=overrides):
                status, data = self.post(
                    "/api/coordinator/pause", self.base_body("pause", **overrides))
                self.assertEqual(status, 400, (overrides, data))
        self.assertEqual(self.control_meta_rows(), [])
        self.assertEqual(self.control_audit_count(), 0)
        _, health = self.get("/api/health")
        item = next(t for t in health["tasks"] if t["task_id"] == self.task_p["id"])
        self.assertEqual(item["control"]["project_intent"], "running")

    def test_bool_or_list_cas_fields_rejected_not_implied(self):
        # expected_epoch/expected_revision 为 bool/数组：共享用例必须严格拒绝，
        # 不得把 True 当 1、['0'] 当 0（否则 CAS 可被类型混淆绕过）。
        for field, value in (("expected_epoch", True), ("expected_epoch", [1]),
                             ("expected_epoch", "1"),
                             ("expected_revision", True),
                             ("expected_revision", ["0"]),
                             ("expected_revision", "0")):
            with self.subTest(field=field, value=value):
                body = self.base_body("pause")
                body[field] = value
                status, data = self.post("/api/coordinator/pause", body)
                self.assertEqual(status, 400, (field, value, data))
        self.assertEqual(self.control_meta_rows(), [])
        _, health = self.get("/api/health")
        item = next(t for t in health["tasks"] if t["task_id"] == self.task_p["id"])
        self.assertEqual(item["control"]["project_intent"], "running")


class CasReplayMultiProjectTests(SafetyWebBase):
    def test_operation_replay_is_idempotent_single_effect(self):
        body = self.base_body("pause", operation_id="op-x")
        s1, d1 = self.post("/api/coordinator/pause", body)
        self.assertEqual(s1, 200, d1)
        self.assertIs(d1["executed"], True)
        self.assertIs(d1["result"]["applied"], True)
        rev1 = d1["result"]["revision"]
        _, health = self.get("/api/health")
        first = next(t for t in health["tasks"]
                     if t["task_id"] == self.task_p["id"])["control"]
        self.assertEqual(first["project_intent"], "paused")
        # 同 operation_id 重放：执行成功但零变更、零额外写
        s2, d2 = self.post("/api/coordinator/pause", body)
        self.assertEqual(s2, 200, d2)
        self.assertIs(d2["executed"], True)
        self.assertIs(d2["result"]["applied"], False)
        self.assertEqual(d2["result"]["revision"], rev1)
        self.assertEqual(self.control_audit_count("coordinator-control.pause"), 1)
        ops = self.conn.execute(
            "SELECT COUNT(*) FROM meta WHERE "
            "key LIKE 'coordinator-control:operation:op-x'").fetchone()[0]
        self.assertEqual(ops, 1)
        _, health2 = self.get("/api/health")
        second = next(t for t in health2["tasks"]
                      if t["task_id"] == self.task_p["id"])["control"]
        self.assertEqual(second["project_revision"], rev1)

    def test_new_operation_with_stale_revision_rejected(self):
        s1, _ = self.post("/api/coordinator/pause",
                          self.base_body("pause", operation_id="op-a"))
        self.assertEqual(s1, 200)
        # 新 operation_id + 旧 revision：CAS 必须拒绝（revision-changed）
        s2, d2 = self.post("/api/coordinator/pause",
                           self.base_body("pause", operation_id="op-b",
                                          expected_revision=0))
        self.assertEqual(s2, 400, d2)
        self.assertEqual(d2["code"], "control_refused")
        _, health = self.get("/api/health")
        item = next(t for t in health["tasks"] if t["task_id"] == self.task_p["id"])
        self.assertEqual(item["control"]["project_intent"], "paused",
                         "CAS 拒绝后状态保持不变")

    def test_multi_project_pause_targets_only_explicit_project(self):
        self.ledger.claim_coordinator(project="q", session_id="ses_coord",
                                      actor="test", ttl=3600)
        task_q = self.ledger.create_task(
            project="q", title="task-q", coordinator_session="ses_coord")
        lease_q = self.ledger.coordinator_lease("q")
        body = self.base_body("pause", project="p",
                              expected_epoch=self.lease_p["epoch"])
        status, data = self.post("/api/coordinator/pause", body)
        self.assertEqual(status, 200, data)
        self.assertIs(data["executed"], True)
        _, health = self.get("/api/health?limit=50")
        by_task = {t["task_id"]: t["control"] for t in health["tasks"]}
        self.assertEqual(by_task[self.task_p["id"]]["project_intent"], "paused")
        self.assertEqual(by_task[task_q["id"]]["project_intent"], "running",
                         "暂停 p 不得波及其他项目")
        # q 的租约/控制不动
        self.assertEqual(self.ledger.coordinator_lease("q")["epoch"], lease_q["epoch"])
        self.assertEqual(self.paused_intents().get(
            "coordinator-control:project:p"), "paused")
        self.assertNotIn("coordinator-control:project:q", self.paused_intents())

    def test_confirm_still_requires_exact_match_even_for_bracket_project(self):
        # 与精确 confirm 同语义：即使目标是字符串化项目名，confirm 仍须逐字一致
        status, data = self.post("/api/coordinator/pause", self.base_body(
            "pause", project="['p']", confirm="p"))
        self.assertEqual(status, 400, data)
        self.assertEqual(data["code"], "CONFIRM_REQUIRED")
        self.assertEqual(self.control_meta_rows(), [])


class PaginationProjectScopeTests(SafetyWebBase):
    def test_coordinator_detail_projects_full_when_tasks_paginated(self):
        # “隐藏项目被误认为唯一项目”的 API 契约：任务分页/截断只影响 tasks，
        # coordinator.projects 始终来自全量任务+租约（前端据此决定是否显示写按钮）。
        self.ledger.create_task(project="p", title="p2", task_id="T-p2",
                                coordinator_session="ses_coord")
        self.ledger.create_task(project="q", title="q1", task_id="T-q1",
                                coordinator_session="ses_coord")
        status, data = self.get("/api/coordinator/ses_coord?limit=1")
        self.assertEqual(status, 200, data)
        self.assertEqual(sorted(data["coordinator"]["projects"]), ["p", "q"])
        self.assertEqual(len(data["tasks"]), 1)
        self.assertTrue(data["tasks_truncated"])
        first = data["tasks"][0]
        self.assertTrue(any(p == first["project"] for p in ("p", "q")))
        # 单项目误判所需的“projects 被分页截断”绝不应发生
        self.assertNotEqual(data["coordinator"]["projects"], [first["project"]])

    def test_health_scope_pagination_does_not_affect_project_identity(self):
        for i in range(3):
            self.ledger.create_task(project="p", title=f"more{i}",
                                    task_id=f"T-m{i}", coordinator_session="ses_coord")
        status, data = self.get("/api/health?session_id=ses_coord&limit=2")
        self.assertEqual(status, 200, data)
        self.assertFalse(data["coverage"]["complete"])
        self.assertIsNotNone(data["coverage"]["next_after_id"])
        # 截断只影响任务页，项目归属维度仍按完整台账（任务都属 p，无“隐藏项目”）
        self.assertTrue(all(t["project"] == "p" for t in data["tasks"]))


def _node_script(home):
    """生成 jsdom 反例脚本（真实 web/src 源码 + 既有 _harness.mjs，非影子实现）。"""
    harness = (WEB / "tests" / "_harness.mjs").resolve()
    return f"""
import {{test}} from "node:test";
import assert from "node:assert/strict";
import {{boot, baseRoutes, Routes, Deferred, coord, tick, waitFor}} from {json.dumps(str(harness))};

function healthOf(sid, projects, {{truncate = false, tasks = null}} = {{}}) {{
  return {{
    ok: true, schema: "oc-orch.continuity-health.v1", scanned: 1,
    observed_at: 1794710000000,
    process: {{healthy: true, connected: true, heartbeat_age_s: 1, last_cycle_at: 1794700000000}},
    semantics: "process-health-not-business-acceptance",
    tasks: tasks ?? projects.map((p, i) => ({{
      task_id: "T-" + sid + "-" + p.replace(/[^a-z]/gi, ""), project: p,
      task_status: "running", state: "no_progress", process_healthy: true,
      business_progress: false, blocked: false, reason: "r", next_action: "n",
      owner: sid, coordinator_session: sid, evidence_coverage: true,
      latest_business_event_at: null, latest_result_version: null,
      responsibilities: [],
      control: {{project_intent: "running", project_revision: 0, session_revision: 0,
                blocking: [], turn_held: false, user_turn_active: false,
                barrier_message_id: null, holder_session: sid, epoch: 1,
                provider_gate: null, provider: null}},
    }})),
    events: [],
    coverage: truncate
      ? {{complete: false, next_after_id: "T-next", cursor_advanced: false, scope: {{session_id: sid}}}}
      : {{complete: true, next_after_id: null, cursor_advanced: false, scope: {{session_id: sid}}}},
    runtime: {{package_version: "0.1.0", schema_version: 3, release_id: null,
              manifest_verified: false, source: "development-or-unmanifested",
              native_trusted_chat_enabled: false, email_default_enabled: false}},
  }};
}}
function coordDetail(sid, projects) {{
  return {{
    ok: true,
    coordinator: coord(sid, {{title: sid, projects,
      leases: projects.map(p => ({{project: p, expires_at: 1799999999999, renewed_at: 0,
        failure_count: 0, recovery_blocked: false, valid: true}}))}}),
    tasks: [], tasks_full_count: 0, tasks_truncated: false, next_after_id: null,
    notifications: [], responsibilities: [], tasks_scope: "",
  }};
}}
function incidentStatus() {{
  return {{total: 0, by_state: {{}}, by_type: {{}}, recipient_refs: [],
          channel: "not-configured", email_configured: false, email_policy_enabled: false}};
}}
function addBase(routes) {{
  routes.get("/api/incidents?limit=50", {{body: {{ok: true, incidents: [],
    status: incidentStatus()}}}});
  return routes;
}}

test("健康异步读：A 慢响应晚到不得串入 B（healthBySid 按 sid 归属）", async () => {{
  const routes = addBase(baseRoutes());
  const slowA = new Deferred();
  routes.set("GET", "/api/health?session_id=ses_a&limit=200", () => slowA.promise);
  routes.get("/api/health?session_id=ses_b&limit=200",
             {{body: healthOf("ses_b", ["/q"])}});
  routes.get("/api/coordinator/ses_a", coordDetail("ses_a", ["/p"]));
  routes.get("/api/coordinator/ses_b", coordDetail("ses_b", ["/q"]));
  routes.get("/api/coordinators", {{ok: true, meta: {{}}, coordinators: [
    coord("ses_a", {{projects: ["/p"]}}), coord("ses_b", {{projects: ["/q"]}})]}});
  const h = await boot(routes, {{hash: "#/"}});
  h.window.location.hash = "#/coordinator/ses_a";
  await tick(30);            // 触发 refreshHealth("ses_a")，慢响应 pending
  h.window.location.hash = "#/coordinator/ses_b";
  await tick(40);            // single-flight：B 的 health 尚未发起（A 在途）
  assert.equal(h.state.healthBySid["ses_a"], undefined,
               "A 在途响应尚未落地任何会话槽位");
  slowA.resolve({{...healthOf("ses_a", ["/p"]), ok: true}});
  await tick(40);            // A 晚到：必须只落 ses_a 槽位
  const aTasks = (h.state.healthBySid["ses_a"]?.body?.tasks || [])
    .map(t => t.task_id);
  assert.ok(aTasks.some(id => id.includes("ses_a")), "A 晚到响应归属 ses_a");
  assert.ok(!aTasks.some(id => id.includes("ses_b")), "A 槽位不混入 B 数据");
  // 在途完成后再拉当前路由（B）：B 拿到自己的数据，且不得覆盖/复用 A 槽位
  await h.refreshLight();
  await tick(40);
  const bTasks = (h.state.healthBySid["ses_b"]?.body?.tasks || [])
    .map(t => t.task_id);
  assert.ok(bTasks.some(id => id.includes("ses_b")), "B 有自己数据");
  assert.ok(!bTasks.some(id => id.includes("ses_a")),
            "B 数据不得含 A 的旧响应（切换 session 不串归属）");
  assert.ok(aTasks.every(id => !id.includes("ses_b")), "A 槽位保持 A 数据");
}});

test("健康截断+多项目：详情全量 projects=[p,q] 时不显示任何写按钮", async () => {{
  const routes = addBase(baseRoutes());
  const sid = "ses_x";
  // 健康页只返回 p 的任务且 coverage.complete=false（截断），但详情 projects 是
  // 全量 [p,q] —— 若前端从健康任务页推断唯一项目会误显“暂停”按钮；反例断言不得显示。
  routes.get("/api/health?session_id=" + sid + "&limit=200",
             {{body: healthOf(sid, ["/p"], {{truncate: true}})}});
  routes.get("/api/coordinator/" + sid, coordDetail(sid, ["/p", "/q"]));
  routes.get("/api/coordinators", {{ok: true, meta: {{}}, coordinators: [
    coord(sid, {{projects: ["/p", "/q"]}})]}});
  const h = await boot(routes, {{hash: "#/coordinator/" + sid}});
  await waitFor(() => h.document.querySelector('[data-tab="health"]') != null,
                {{timeout: 6000}});
  h.document.querySelector('[data-tab="health"]').click();
  await waitFor(() => h.el.textContent.includes("业务健康"), {{timeout: 6000}});
  const btns = [...h.document.querySelectorAll('[data-act="projpause"], [data-act="projresume"]')];
  assert.equal(btns.length, 0, "多项目（健康截断只显示其一）不得出现默认写按钮");
  assert.match(h.el.textContent, /多项目关联[：:]/,
               "应明确提示多项目不猜测目标项目");
}});

test("验收草稿：task 与 attempt 键隔离，返回不串旧值", async () => {{
  const routes = addBase(baseRoutes());
  const taskBody = (id, attemptId) => ({{
    ok: true,
    task: {{id, project: "p", title: "T" + id, objective: "o", acceptance: "a",
          status: "running", coordinator_source: "explicit",
          effective_coordinator: "ses_c", current_attempt_id: attemptId,
          coordinator_session: "ses_c"}},
    attempts: [{{id: attemptId, generation: 1, status: "result_received",
                dispatch_state: "confirmed", session_id: "ses_w", verification: {{}}}}],
    attempts_full_count: 1, attempts_truncated: false, notifications: [],
  }});
  routes.get("/api/task/T-1", taskBody("T-1", "A-1"));
  routes.get("/api/task/T-2", taskBody("T-2", "A-2"));
  routes.get("/api/attempt/A-1", {{
    ok: true,
    attempt: {{id: "A-1", task_id: "T-1", status: "result_received",
              dispatch_state: "confirmed", verification: {{}}}},
    task: {{id: "T-1", project: "p", title: "T1", status: "running"}},
    deliverables: [],
  }});
  const h = await boot(routes, {{hash: "#/task/T-1"}});
  const type = async (value) => {{
    const input = h.document.querySelector('form[data-form="verify"] [name="evidence"]');
    input.value = value;
    input.dispatchEvent(new h.window.Event("input", {{bubbles: true}}));
    await tick(10);
  }};
  const clickVerify = async (pass) => {{
    h.document.querySelector('[data-act="verify"][data-pass="' + (pass ? 1 : 0) + '"]').click();
    await waitFor(() => h.document.querySelector('form[data-form="verify"]') != null,
                  {{timeout: 6000}});
  }};
  await waitFor(() => h.document.querySelector('[data-act="verify"]') != null,
                {{timeout: 6000}});
  await clickVerify(true);
  await type("TASK-ONLY");
  assert.equal(h.state.verifyDrafts.get("task:T-1").evidence, "TASK-ONLY");
  // 切到另一任务：新表单不得显示旧任务草稿值
  h.window.location.hash = "#/task/T-2";
  await tick(40);
  await clickVerify(true);
  const emptyInput = h.document.querySelector('form[data-form="verify"] [name="evidence"]');
  assert.equal(emptyInput.value, "", "打开另一任务后不得串入旧 evidence");
  assert.equal(h.state.verifyDrafts.get("task:T-2"), undefined);
  await clickVerify(true);
  await type("SECOND");
  assert.equal(h.state.verifyDrafts.get("task:T-2").evidence, "SECOND");
  // 回 T-1：恢复 task:T-1 自己的草稿（不被 task:T-2 覆盖）
  h.window.location.hash = "#/task/T-1";
  await tick(40);
  await waitFor(() => h.document.querySelector('form[data-form="verify"] [name="evidence"]') != null,
                {{timeout: 6000}});
  const back1 = h.document.querySelector('form[data-form="verify"] [name="evidence"]');
  assert.equal(back1.value, "TASK-ONLY", "返回原任务恢复原草稿，不串 task:T-2 的值");
  // attempt 键（attempt:A-1）独立：草稿键与 task:T-1 隔离
  h.window.location.hash = "#/attempt/A-1";
  await tick(40);
  await waitFor(() => h.document.querySelector('[data-act="verify"]') != null,
                {{timeout: 6000}});
  await clickVerify(false);
  await type("ATTEMPT-ONLY");
  assert.equal(h.state.verifyDrafts.get("attempt:A-1").evidence, "ATTEMPT-ONLY");
  assert.equal(h.state.verifyDrafts.get("task:T-1").evidence, "TASK-ONLY",
               "task 与 attempt 键草稿互相隔离");
}});
"""
# noqa


class FrontendJsdomCounterTests(unittest.TestCase):
    """真实 web/src 源码 + 既有 jsdom harness 反例（node --test 子进程）。"""

    def test_frontend_jsdom_counterexamples_pass(self):
        node = shutil.which("node")
        assert node, "需要 node 运行真实前端 harness"
        tmp = tempfile.mkdtemp(prefix="web-safety-js-", dir="/tmp/opencode")
        self.addCleanup(lambda: shutil.rmtree(tmp, ignore_errors=True))
        script = Path(tmp) / "web_safety_jsdom.mjs"
        script.write_text(_node_script(tmp), encoding="utf-8")
        proc = subprocess.run(
            [node, "--test", str(script)], cwd=str(WEB), text=True,
            capture_output=True, timeout=180)
        if proc.returncode != 0:
            self.fail(
                "前端 jsdom 反例失败（exit=%d）：\n%s\n%s"
                % (proc.returncode, proc.stdout[-4000:], proc.stderr[-4000:]))


if __name__ == "__main__":
    unittest.main(verbosity=2)

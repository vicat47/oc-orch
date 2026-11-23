/**
 * R2 前端真实 DOM：业务健康表（进程≠业务）、暂停/继续控制（真实 CAS 版本 + confirm）、
 * 本地人工待办 incidents 展示、运行身份（仅用 health.runtime）、以及验收表单草稿保全
 * （提交不再依赖易失 DOM 节点值 —— 后台刷新换节点也不能丢 evidence）。
 *
 * 正反例覆盖：单项目显示控制按钮、多项目/无项目不猜、read_only 隐藏写按钮、
 * 确认不匹配不发请求、后端拒绝不显示成功、incidents 只读不 reconcile/不发邮件。
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { boot, bootBundle, baseRoutes, coord, snapshot, tick, waitFor } from "./_harness.mjs";

function healthBody(overrides = {}) {
  return {
    ok: true, schema: "oc-orch.continuity-health.v1", scanned: 1,
    observed_at: 1794710000000,
    process: {healthy: true, connected: true, heartbeat_age_s: 1, last_cycle_at: 1794700000000},
    semantics: "process-health-not-business-acceptance",
    tasks: [{
      task_id: "T-h1", project: "/p", task_status: "running", state: "no_progress",
      process_healthy: true, business_progress: false, blocked: false,
      reason: "待登记责任", next_action: "登记责任", owner: "ses_x",
      coordinator_session: "ses_x", evidence_coverage: true,
      latest_business_event_at: null, latest_result_version: null,
      responsibilities: [{
        id: "R-1", state: "waiting", owner: "ses_x", wait_kind: "dependency",
        next_action: "继续等依赖", next_check_at: 1794710001000,
        deadline: 1794710060000, condition_version: 1, budget_used: 1, max_budget: 3,
      }],
      control: {
        project_intent: "running", project_revision: 0, session_revision: 0,
        blocking: [], turn_held: false, user_turn_active: false,
        barrier_message_id: null, holder_session: "ses_x", epoch: 1,
        provider_gate: null, provider: null,
      },
    }],
    events: [],
    coverage: {complete: true, next_after_id: null, cursor_advanced: false,
               scope: {project: null, session_id: "ses_x"}},
    runtime: {
      package_version: "0.1.0", schema_version: 3, release_id: null,
      manifest_verified: false, source: "development-or-unmanifested",
      native_trusted_chat_enabled: false, email_default_enabled: false,
    },
    ...overrides,
  };
}

function incidentStatus() {
  return {total: 1, by_state: {open: 1}, by_type: {"no-progress": 1},
          recipient_refs: [], channel: "not-configured",
          email_configured: false, email_policy_enabled: false};
}

function coordDetail(sid, projects) {
  return {
    ok: true,
    coordinator: coord(sid, {title: `协调者${sid}`, projects, leases: projects.map(p => ({
      project: p, expires_at: 1799999999999, renewed_at: 0, failure_count: 0,
      recovery_blocked: false, valid: true,
    }))}),
    tasks: [], tasks_full_count: 0, tasks_truncated: false, next_after_id: null,
    notifications: [], responsibilities: [], tasks_scope: "",
  };
}

function addHealthRoutes(routes, sid = "ses_x", projects = ["/p"]) {
  routes.get("/api/health", healthBody());
  // 注意 harness 约定：spec 顶层 status 视为 HTTP 状态码 → incidents 的业务 status 必须包 body
  routes.get("/api/incidents?limit=50", {body: {ok: true, incidents: [], status: incidentStatus()}});
  routes.get(`/api/coordinator/${sid}`, coordDetail(sid, projects));
  routes.get("/api/coordinators", {ok: true, meta: {}, coordinators: [coord(sid, {projects})]});
  return routes;
}

// 打开“健康”tab 并等待业务健康表内容；健康数据由 refreshHealth 在 boot/刷新时已拉取。
async function openHealthTab(h) {
  await waitFor(() => h.document.querySelector('[data-tab="health"]') != null,
               {timeout: 6000});
  h.document.querySelector('[data-tab="health"]').click();
  await waitFor(() => h.el.textContent.includes("业务健康"), {timeout: 6000});
}

function clearToast(h) {
  const node = h.document.querySelector("#toast");
  if (node) node.textContent = "";
}

test("工作台健康：业务健康表渲染（owner/next/期限/预算/证据/控制），进程健康≠业务完成", async () => {
  const routes = addHealthRoutes(baseRoutes());
  const h = await boot(routes, {hash: "#/coordinator/ses_x"});
  await openHealthTab(h);
  const text = h.el.textContent;
  assert.ok(text.includes("T-h1"), "健康表含任务");
  assert.ok(text.includes("ses_x"), "owner 展示");
  assert.ok(text.includes("登记责任"), "下一动作展示（任务级）");
  assert.ok(text.includes("1/3"), "预算 budget 展示");
  assert.ok(text.includes("进程 healthy"), "进程健康展示");
  assert.ok(text.includes("进程健康 ≠ 业务进展/完成"), "语义提示");
  assert.ok(text.includes("邮件默认关闭"), "health.runtime 邮件默认关闭提示");
  assert.ok(text.includes("native 对话未启用"), "health.runtime native 提示");
});

test("工作台健康：单项目显示暂停按钮；点击携带真实 revision/epoch 与精确 confirm", async () => {
  const routes = addHealthRoutes(baseRoutes());
  let posted = null;
  routes.post("/api/coordinator/pause", (req) => {
    posted = JSON.parse(String(req.init?.body ?? req.init?.data ?? "{}"));
    return {body: {ok: true, executed: true, action: "pause", project: posted.project,
                   session_id: posted.session_id,
                   result: {executed: true, applied: true, revision: 1, epoch: 1}}};
  });
  const h = await boot(routes, {hash: "#/coordinator/ses_x", prompt: "/p"});
  await openHealthTab(h);
  const pauseBtn = h.document.querySelector('[data-act="projpause"]');
  assert.ok(pauseBtn, "单项目且 intent=running 应显示暂停按钮");
  assert.equal(h.document.querySelector('[data-act="projresume"]'), null, "running 不显示继续");
  pauseBtn.click();
  await waitFor(() => posted != null);
  assert.equal(posted.project, "/p");
  assert.equal(posted.session_id, "ses_x");
  assert.equal(posted.expected_revision, 0, "CAS revision 取健康投影真实值");
  assert.equal(posted.expected_epoch, 1, "CAS epoch 取健康投影真实值");
  assert.equal(posted.confirm, "/p", "confirm 精确项目");
  assert.ok(posted.operation_id.startsWith("web-pause-"));
  await tick(80); // 等 POST 触发的后台刷新落定，避免污染下一用例的单飞 refreshLight
});

test("工作台健康：确认输入不匹配 → 不发请求", async () => {
  const routes = addHealthRoutes(baseRoutes());
  routes.post("/api/coordinator/pause", {body: {ok: true, executed: true}});
  const h = await boot(routes, {hash: "#/coordinator/ses_x", prompt: "WRONG"});
  await openHealthTab(h);
  h.document.querySelector('[data-act="projpause"]')?.click();
  await tick(80);
  assert.equal(routes.count((c) => c.path === "/api/coordinator/pause"), 0,
               "确认不匹配不得发 POST");
  assert.equal(h.window.__dialogs.prompts.length, 1, "应弹出确认输入框");
});

test("工作台健康：intent=paused 显示继续按钮并 POST /resume", async () => {
  const routes = addHealthRoutes(baseRoutes());
  const pausedTask = {
    ...healthBody().tasks[0],
    control: {...healthBody().tasks[0].control, project_intent: "paused",
              project_revision: 2, epoch: 1},
  };
  routes.get("/api/health", healthBody({tasks: [pausedTask]}));
  let posted = null;
  routes.post("/api/coordinator/resume", (req) => {
    posted = JSON.parse(String(req.init?.body ?? req.init?.data ?? "{}"));
    return {body: {ok: true, executed: true, result: {applied: true, revision: 3, epoch: 1}}};
  });
  const h = await boot(routes, {hash: "#/coordinator/ses_x", prompt: "/p"});
  await openHealthTab(h);
  assert.equal(h.document.querySelector('[data-act="projpause"]'), null, "paused 不显示暂停");
  const resumeBtn = h.document.querySelector('[data-act="projresume"]');
  assert.ok(resumeBtn, "paused 显示继续按钮");
  resumeBtn.click();
  await waitFor(() => posted != null);
  assert.equal(posted.project, "/p");
  assert.equal(posted.expected_revision, 2, "继续也读真实 revision");
  assert.equal(posted.expected_epoch, 1);
  assert.equal(posted.confirm, "/p");
  await tick(80);
});

test("工作台健康：多项目关联不猜测，不显示控制按钮", async () => {
  const multi = addHealthRoutes(baseRoutes(), "ses_multi", ["/p1", "/p2"]);
  const hm = await boot(multi, {hash: "#/coordinator/ses_multi"});
  await openHealthTab(hm);
  assert.equal(hm.document.querySelector('[data-act="projpause"]'), null, "多项目不猜测");
  assert.equal(hm.document.querySelector('[data-act="projresume"]'), null, "多项目不猜测");
  assert.ok(hm.el.textContent.includes("多项目关联"), "给出不猜测说明");
});

test("工作台健康：无项目不显示控制按钮", async () => {
  const none = addHealthRoutes(baseRoutes(), "ses_none", []);
  const hn = await boot(none, {hash: "#/coordinator/ses_none"});
  await openHealthTab(hn);
  assert.equal(hn.document.querySelector('[data-act="projpause"]'), null, "无项目不显示");
  assert.equal(hn.document.querySelector('[data-act="projresume"]'), null, "无项目不显示");
  assert.ok(hn.el.textContent.includes("没有可归属项目"), "给出不显示说明");
});

test("read_only 下健康表不显示任何写按钮", async () => {
  const routes = addHealthRoutes(baseRoutes());
  routes.get("/api/status", {ok: true, snapshot: snapshot({read_only: true})});
  const h = await boot(routes, {hash: "#/coordinator/ses_x"});
  await openHealthTab(h);
  assert.equal(h.document.querySelector('[data-act="projpause"]'), null, "read_only 无暂停");
  assert.equal(h.document.querySelector('[data-act="projresume"]'), null, "read_only 无继续");
});

test("后端拒绝暂停不显示成功：toast 不含已应用", async () => {
  const routes = addHealthRoutes(baseRoutes());
  routes.post("/api/coordinator/pause", {
    status: 400, body: {ok: false, code: "control_refused", message: "CAS 版本已变化"},
  });
  const h = await boot(routes, {hash: "#/coordinator/ses_x", prompt: "/p"});
  await openHealthTab(h);
  clearToast(h); // 清掉上一用例残留 toast，避免误判成功
  h.document.querySelector('[data-act="projpause"]')?.click();
  await waitFor(() => (h.document.querySelector("#toast")?.textContent || "").length > 0);
  const toastText = h.document.querySelector("#toast").textContent;
  assert.ok(!toastText.includes("已应用") && !toastText.includes("已暂停"),
            "拒绝时不得显示成功：" + toastText);
  assert.ok(toastText.includes("CAS"), "显示失败原因");
  await tick(80);
});

test("诊断 incidents：owner/next_action/channel 展示且只读（无 reconcile/发信 POST）", async () => {
  const routes = baseRoutes();
  routes.get("/api/health?limit=50", healthBody());
  routes.get("/api/incidents?limit=50", {body: {ok: true, incidents: [{
    incident_id: "INC-1", project: "/p", incident_type: "no-progress",
    source_kind: "responsibility", source_id: "R-1", task_id: "T-h1",
    condition_key: "decision", escalation_level: 1, state: "open",
    owner: "operator", next_action: "确认 owner", blocked_reason: null,
    first_seen_at: 1794700000000, last_observed_at: 1794700001000,
    deadline: 1794710060000, channel: "not-configured", recipient_ref: "",
  }], status: incidentStatus()}});
  const h = await boot(routes, {hash: "#/diagnostics"});
  await waitFor(() => h.el.textContent.includes("INC-1"), {timeout: 6000});
  const text = h.el.textContent;
  assert.ok(text.includes("确认 owner"), "next_action 展示");
  assert.ok(text.includes("operator"), "owner 展示");
  assert.ok(text.includes("not-configured"), "通道状态展示");
  assert.ok(text.includes("邮件默认关闭"), "邮件默认关闭提示");
  assert.equal(routes.count((c) => c.method === "POST"), 0, "只读不 reconcile/不发送");
});

test("验收表单：后台刷新/重建输入节点后提交仍带正确 evidence（草稿保全）", async () => {
  const routes = baseRoutes();
  routes.get("/api/task/T-1", {
    ok: true,
    task: {id: "T-1", project: "/p", title: "T", objective: "o", acceptance: "a",
           status: "running", coordinator_source: "explicit",
           effective_coordinator: "ses_c", current_attempt_id: "A-1",
           coordinator_session: "ses_c"},
    attempts: [{id: "A-1", generation: 1, status: "result_received",
                dispatch_state: "confirmed", verification: {}, result_path: null}],
    attempts_full_count: 1, attempts_truncated: false, notifications: [],
  });
  let verifyBody = null;
  routes.post("/api/attempt/A-1/verify", (req) => {
    verifyBody = JSON.parse(String(req.init?.body ?? req.init?.data ?? "{}"));
    return {body: {ok: true, result: {ok: true}}};
  });
  const h = await boot(routes, {hash: "#/task/T-1"});
  await waitFor(() => h.el.textContent.includes("A-1"));
  h.document.querySelector('[data-act="verify"][data-pass="1"]')?.click();
  await waitFor(() => h.document.querySelector('form[data-form="verify"]') != null);
  const form = h.document.querySelector('form[data-form="verify"]');
  const evidence = form.elements.evidence;
  evidence.value = "e1; e2; e3";
  evidence.dispatchEvent(new h.window.Event("input", {bubbles: true}));
  form.elements.confirm.value = "A-1";
  form.elements.confirm.dispatchEvent(new h.window.Event("input", {bubbles: true}));
  // 模拟后台刷新重建表单节点（未受控输入值被清空）——草稿必须兜住
  const fresh = form.cloneNode(true);
  form.replaceWith(fresh);
  fresh.querySelector('button[type="submit"]').click();
  await waitFor(() => verifyBody != null);
  assert.deepEqual(verifyBody.evidence, ["e1", "e2", "e3"], "草稿保全：提交不丢 evidence");
  assert.equal(verifyBody.confirm, "A-1");
  assert.equal(verifyBody.force, false);
});

test("打包产物：诊断页包含业务健康与本地人工待办区块（bundle 离线）", async () => {
  const routes = baseRoutes();
  routes.get("/api/health?limit=50", healthBody());
  routes.get("/api/incidents?limit=50", {body: {ok: true, incidents: [], status: incidentStatus()}});
  const h = await bootBundle(routes, {hash: "#/diagnostics"});
  await waitFor(() => h.el.textContent.includes("业务健康"), {timeout: 8000});
  assert.ok(h.el.textContent.includes("本地人工待办"), "bundle 应有 incidents 区块");
  assert.ok(h.el.textContent.includes("进程健康 ≠ 业务进展/完成"));
});
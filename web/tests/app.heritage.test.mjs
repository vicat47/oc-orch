/**
 * 旧 TestPageJsBehavior 四项核心业务语义的持久化迁移（真实 DOM），
 * 以及 toast 唯一性（外壳 + 应用合成真实 DOM 恰 1 个 #toast）。父读审映射提醒：
 * notify-only 身份 ≠ read_only/XSS，合并稳定顺序/输入失败保留/解锁委托均为独立覆盖，
 * 不能以“测试数量”替代，以下逐项保持旧语义。
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import {
  boot, baseRoutes, coord, snapshot, waitFor, tick, packagedShell,
} from "./_harness.mjs";

function withCoords(...coords) {
  return {ok: true, meta: {}, coordinators: coords};
}

function coordinatorDetail(sid, overrides = {}) {
  return {
    ok: true,
    coordinator: coord(sid, {title: `协调者${sid}`}),
    tasks: [], tasks_full_count: 0, tasks_truncated: false, next_after_id: null,
    notifications: [], responsibilities: [], tasks_scope: "",
    ...overrides,
  };
}

test("合并稳定顺序：服务端 [new,b,a] 后客户端仍 [a,b,new] 且原卡片节点保持", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(
    coord("ses_a", {title: "甲", created_at: 100}),
    coord("ses_b", {title: "乙", created_at: 200}),
  ));
  const h = await boot(routes);
  const cardA = await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_a"]'));
  await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_b"]'));
  const order = () => [...h.el.querySelectorAll('[data-key^="coordinator:"]')]
    .map((el) => el.dataset.key.replace("coordinator:", ""));

  assert.deepEqual(order(), ["ses_a", "ses_b"], "首次按服务端顺序（创建时间升序）");
  // 服务端下一轮把 new 提到最前：客户端必须保持首见顺序，new 追加在末尾。
  routes.get("/api/coordinators", withCoords(
    coord("ses_new", {title: "新", created_at: 300}),
    coord("ses_b", {title: "乙", created_at: 200}),
    coord("ses_a", {title: "甲", created_at: 100}),
  ));
  await h.refreshLight();
  await tick();
  assert.deepEqual(order(), ["ses_a", "ses_b", "ses_new"],
                   "刷新绝不重排已见对象；新增对象追加末尾");
  assert.equal(h.el.querySelector('[data-key="coordinator:ses_a"]'), cardA,
               "原会话卡片节点实例必须保持（keyed 更新）");
});

test("身份标注：notify-only 空标题不得称未命名调度者，confirmed 显示已关联", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(
    coord("ses_n", {
      title: "", counts: {notifications_pending: 2}, gates: [], leases: [],
      sources: ["notify:ses_n"], identity: "notify-associated",
    }),
    coord("ses_c", {
      title: "真调度者", counts: {}, gates: [], leases: [], projects: ["/p"],
      sources: ["task:T-1"], identity: "confirmed",
    }),
  ));
  const h = await boot(routes);
  await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_n"]'));
  const text = h.el.textContent;
  assert.ok(text.includes("仅通知关联 · 调度者角色未确认"),
            "notify-only 必须以角色未确认标注");
  assert.ok(text.includes("通知关联会话（未确认调度者）"),
            "notify-only 空标题不得称未命名调度者");
  assert.ok(text.includes("已关联调度者"), "task 来源显示已关联调度者");
  assert.ok(!text.includes("未命名调度者"), "本页不存在未命名调度者字样");
});

test("输入接口失败保留旧 state.inputs 并标 extraErr，恢复后清除", async () => {
  const routes = baseRoutes();
  routes.get("/api/inputs?limit=50", {ok: true, inputs: [
    {input_id: "I-old", session_id: "ses_w", intent: "x", owner: "o"},
  ]});
  const h = await boot(routes);
  await waitFor(() => h.state.inputs.length === 1);

  routes.get("/api/inputs?limit=50", {status: 500, body: {message: "boom"}});
  await h.refreshLight();
  assert.equal(h.state.extraErr, true, "输入源失败必须置 extraErr");
  assert.equal(h.state.inputs.length, 1, "失败必须保留旧 state.inputs");

  routes.get("/api/inputs?limit=50", {ok: true, inputs: [
    {input_id: "I-old", session_id: "ses_w", intent: "x", owner: "o"},
    {input_id: "I-new", session_id: "ses_w", intent: "y", owner: "o"},
  ]});
  await h.refreshLight();
  assert.equal(h.state.extraErr, false, "恢复轮必须清除 extraErr");
  assert.equal(h.state.inputs.length, 2, "恢复后 inputs 更新为新集合");
});

test("解锁：一次点击一次 POST（project/session/reason/UNLOCK 完整），read_only 不发", async () => {
  const gated = coord("ses_a", {
    gates: [{state: "manual-blocked", source: "manual"}],
    leases: [{project: "/p", valid: true, expires_at: Date.now() + 60000}],
  });
  const routes = baseRoutes();
  routes.get("/api/coordinator/ses_a", coordinatorDetail("ses_a", {
    coordinator: gated,
  }));
  routes.post("/api/coordinator/unlock", {ok: true, result: {cleared: true, resurfaced: 0}});
  const h = await boot(routes, {hash: "#/coordinator/ses_a", prompt: () => "UNLOCK"});

  const btn = await waitFor(() => h.el.querySelector('[data-act="unlock"]'));
  btn.click();
  await tick();
  const posts = routes.calls((c) => c.path === "/api/coordinator/unlock" && c.method === "POST");
  assert.equal(posts.length, 1, "一次点击应恰好一次解锁 POST");
  assert.deepEqual(JSON.parse(posts[0].init.body), {
    project: "/p", session: "ses_a", reason: "UNLOCK", confirm: "UNLOCK",
  }, "解锁必须回填 project/session/reason/UNLOCK 完整集合");

  // read_only：解锁按钮禁用，点击不发请求
  const routesRo = baseRoutes();
  routesRo.get("/api/status", {ok: true, snapshot: snapshot({read_only: true})});
  routesRo.get("/api/coordinator/ses_a", coordinatorDetail("ses_a", {coordinator: gated}));
  routesRo.post("/api/coordinator/unlock", {ok: true});
  const hRo = await boot(routesRo, {hash: "#/coordinator/ses_a", prompt: () => "UNLOCK"});
  const btnRo = await waitFor(() => hRo.el.querySelector('[data-act="unlock"]'));
  assert.ok(btnRo.disabled, "read_only 解锁按钮应禁用");
  btnRo.click();
  await tick();
  assert.equal(routesRo.count((c) => c.path === "/api/coordinator/unlock" && c.method === "POST"),
               0, "read_only 点击解锁不得发出请求");
});

test("provider 闸门：恢复调度按钮带 allow_provider，缺失时不 POST（read_only/非阻塞态无动作）", async () => {
  const gated = coord("ses_p", {
    gates: [{state: "blocked", source: "provider", reason: "no credits"}],
    leases: [{project: "/p", valid: true, expires_at: Date.now() + 60000}],
    projects: ["/p"],
  });
  const routes = baseRoutes();
  routes.get("/api/coordinator/ses_p", coordinatorDetail("ses_p", {coordinator: gated}));
  routes.post("/api/coordinator/unlock", {ok: true, result: {cleared: true, resurfaced: 0, cleared_via: "provider"}});
  const h = await boot(routes, {hash: "#/coordinator/ses_p", prompt: () => "UNLOCK"});

  const btn = await waitFor(() => h.el.querySelector('[data-act="unlock"]'));
  assert.ok(btn.textContent.includes("恢复调度"), "provider 闸门按钮应标注‘恢复调度’");
  assert.equal(btn.dataset.allowProvider, "1", "provider 恢复调度必须携带 allow-provider 标记");
  btn.click();
  await tick();
  const posts = routes.calls((c) => c.path === "/api/coordinator/unlock" && c.method === "POST");
  assert.equal(posts.length, 1, "一次点击应恰好一次解锁 POST");
  assert.deepEqual(JSON.parse(posts[0].init.body), {
    project: "/p", session: "ses_p", reason: "UNLOCK", confirm: "UNLOCK", allow_provider: true,
  }, "provider 解锁必须显式 allow_provider（operator 动作）");
});

test("toast 唯一且带 class=toast（外壳不再声明，应用渲染恰一个）", async () => {
  const shell = packagedShell();
  assert.ok(!/<div\s+id="toast"[^>]*>/.test(shell), "页壳不得再声明 #toast（避免重复 id）");
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(coord("ses_a")));
  const h = await boot(routes);
  await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_a"]'));
  const toasts = h.document.querySelectorAll("#toast");
  assert.equal(toasts.length, 1, "页面（外壳+应用）上 #toast 必须唯一");
  assert.ok(toasts[0].classList.contains("toast"), 'toast 需带 class="toast"');
  assert.equal(toasts[0].getAttribute("role"), "status");
  assert.equal(toasts[0].getAttribute("aria-live"), "polite");
});
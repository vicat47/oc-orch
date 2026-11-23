import assert from "node:assert/strict";
import {test} from "node:test";
import {boot, baseRoutes, Deferred, tick, waitFor, snapshot} from "./_harness.mjs";

function view(scope = "task", target = "T-policy", overrides = {}) {
  return {ok: true, scope, scope_id: target, managed: true, revision: 2,
    version: {policy: {preset: "elastic", stage: "standard", limits: {max_parallel: 4, max_candidates: 2, max_challenge_rounds: 1}}},
    execution_enforced: false, authorization: {revision: 0}, inflight: {items: [], total: 0}, ...overrides};
}
function routesFor(overrides = {}) {
  const routes = baseRoutes();
  routes.get("/api/task/T-policy", {ok: true, task: {id: "T-policy", project: "p", title: "策略任务", status: "running"},
    attempts: [], delivery_policy: view(), project_delivery_policy: view("project", "p"), ...overrides});
  return routes;
}
function fill(h, form, name, value) {
  const el = form.elements[name];
  el.value = value;
  el.dispatchEvent(new h.window.Event("input", {bubbles: true}));
}
async function ready(routes) {
  const h = await boot(routes, {hash: "#/task/T-policy"});
  h.window.dispatchEvent(new h.window.Event("hashchange"));
  const form = await waitFor(() => h.el.querySelector('form[data-scope="task"]'));
  fill(h, form, "reason", "explicit preference");
  fill(h, form, "confirm", "T-policy");
  return {h, form};
}

test("草案与原型/费用/在途边界可见，未提供覆盖不伪报启用", async () => {
  const {h} = await ready(routesFor());
  assert.match(h.el.textContent, /运行时守卫尚未启用/);
  assert.match(h.el.textContent, /共用协调者费用独立/);
  assert.match(h.el.textContent, /按既定标准验收/);
  assert.match(h.el.textContent, /新任务的项目默认策略/);
});

test("确认逐字匹配、取消不POST；预算/阶段和三个上限独立提交", async () => {
  const routes = routesFor();
  routes.post("/api/delivery-policy/task", ({init}) => {
    const body = JSON.parse(init.body);
    return {ok: true, applied: true, operation_id: body.operation_id, applied_revision: 3, current: view("task", "T-policy", {revision: 3})};
  });
  const {h, form} = await ready(routes);
  fill(h, form, "confirm", " T-policy ");
  form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true}));
  await tick();
  assert.equal(routes.calls(c => c.method === "POST").length, 0);
  fill(h, form, "confirm", "T-policy");
  fill(h, form, "stage", "prototype");
  form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true}));
  await waitFor(() => routes.calls(c => c.method === "POST").length === 1);
  const body = JSON.parse(routes.calls(c => c.method === "POST")[0].init.body);
  assert.equal(body.policy.preset, "elastic");
  assert.equal(body.policy.stage, "prototype");
  assert.equal(body.expected_revision, 2);
  assert.deepEqual(body.policy.limits, {max_parallel: 4, max_candidates: 2, max_challenge_rounds: 1});
});

test("网络未知保原operation/body，重复提交不POST；只读GET核对才清除", async () => {
  const routes = routesFor();
  routes.post("/api/delivery-policy/task", () => { throw new Error("lost response"); });
  const {h, form} = await ready(routes);
  form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true}));
  const button = await waitFor(() => h.el.querySelector('[data-act="policyrecheck"]'));
  await tick();
  const posts = routes.calls(c => c.method === "POST");
  assert.equal(posts.length, 1);
  const operation = JSON.parse(posts[0].init.body).operation_id;
  const currentForm = h.el.querySelector('form[data-scope="task"]');
  currentForm.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true}));
  await tick();
  assert.equal(routes.calls(c => c.method === "POST").length, 1);
  routes.get("/api/delivery-policy", view("task", "T-policy", {revision: 3, version: {operation_id: operation}}));
  button.click();
  await waitFor(() => !h.el.querySelector('[data-act="policyrecheck"]'));
  assert.equal(routes.calls(c => c.method === "POST").length, 1);
  assert.ok(routes.calls(c => c.path === "/api/delivery-policy" && c.method === "GET").length);
});

test("多次点击单飞，旧回执不清用户继续输入的新草稿", async () => {
  const routes = routesFor();
  const pending = new Deferred();
  routes.post("/api/delivery-policy/task", pending.promise);
  const {h, form} = await ready(routes);
  form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true}));
  form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true}));
  await waitFor(() => routes.calls(c => c.method === "POST").length === 1);
  const active = h.el.querySelector('form[data-scope="task"]');
  fill(h, active, "reason", "edited during request");
  const body = JSON.parse(routes.calls(c => c.method === "POST")[0].init.body);
  pending.resolve({ok: true, operation_id: body.operation_id, applied_revision: 3, current: view("task", "T-policy", {revision: 4})});
  await waitFor(() => !h.el.querySelector('[data-act="policyrecheck"]'));
  assert.equal(h.el.querySelector('form[data-scope="task"]').elements.reason.value, "edited during request");
});

test("read_only隐藏策略mutation；损坏状态不呈现可写表单", async () => {
  const routes = routesFor({delivery_policy: {ok: false, code: "DELIVERY_POLICY_CORRUPT"}});
  routes.get("/api/status", {ok: true, snapshot: snapshot({read_only: true})});
  const h = await boot(routes, {hash: "#/task/T-policy"});
  h.window.dispatchEvent(new h.window.Event("hashchange"));
  await waitFor(() => h.el.textContent.includes("DELIVERY_POLICY_CORRUPT"));
  assert.equal(h.el.querySelector('form[data-form="delivery-policy"]'), null);
  assert.equal(routes.calls(c => c.method === "POST").length, 0);
});

test("旧版在途即使交接过期仍显示可能付费，不误报停止或退款", async () => {
  const routes = routesFor({delivery_policy: view("task", "T-policy", {execution_enforced: true,
    inflight: {total: 1, items: [{attempt_id: "A-old", message_id: "msg_original", policy_revision: 1,
      model: "fixture/paid", cost: "paid", issued_at: 1, older_policy: true}]}})});
  const {h} = await ready(routes);
  assert.match(h.el.textContent, /使用旧策略/);
  assert.match(h.el.textContent, /msg_original/);
  assert.match(h.el.textContent, /不承诺撤回\/退费/);
});

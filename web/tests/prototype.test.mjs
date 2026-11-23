import assert from "node:assert/strict";
import {test} from "node:test";
import {boot, baseRoutes, waitFor, tick, Deferred} from "./_harness.mjs";

function view() {
  return {ok: true, task_id: "T-prototype", revision: 1, commitment: {objective: "原承诺", acceptance: ["正式检查"]},
    unresolved: ["gap-1"], current: {plan: {question: "验证菜单体验", entry: "#/task/T-prototype", startup: "隔离启动，不执行文本",
      checks: ["注册检查尚须运行"], paths: [{id: "main", features: ["mock", "pending", "blocked"]}],
      features: [{id: "mock", title: "模拟保存", state: "mock", source: "固定样例，零真实写入", gap_ids: ["gap-1"]},
        {id: "pending", title: "导出菜单", state: "placeholder", source: "未实现", gap_ids: ["gap-1"]},
        {id: "blocked", title: "接口错误", state: "blocked", source: "后端不可用，不当空数据", gap_ids: ["gap-1"]}],
      gaps: [{id: "gap-1", kind: "defect", state: "open", risk: "ordinary", expected: "真实后端", current: "未接通",
        reason: "原型范围", impact: "不能正式保存", owner: "operator", next_action: "实现并联调", review_at: "2026-11-17",
        refs: ["本Task"], followup_task: null, evidence: null}]}}};
}

function routesFor(prototype = view()) {
  const routes = baseRoutes();
  routes.get("/api/task/T-prototype", {ok: true, task: {id: "T-prototype", project: "p", title: "原型", status: "running"}, attempts: [], prototype});
  return routes;
}
function fill(h, form, name, value) {
  form.elements[name].value = value;
  form.elements[name].dispatchEvent(new h.window.Event("input", {bubbles: true}));
}
async function ready(routes = routesFor()) {
  const h = await boot(routes, {hash: "#/task/T-prototype"});
  h.window.dispatchEvent(new h.window.Event("hashchange"));
  const form = await waitFor(() => h.el.querySelector('form[data-form="prototype"]'));
  fill(h, form, "reason", "原型反馈"); fill(h, form, "confirm", "T-prototype");
  return {h, form};
}

test("P17/P18代表路径可点，mock/占位/受阻有反馈，零业务POST与假成功", async () => {
  const routes = routesFor(); const {h} = await ready(routes);
  for (const id of ["mock", "pending", "blocked"]) {
    h.el.querySelector(`[data-feature="${id}"]`).click();
    const panel = await waitFor(() => h.el.querySelector('[data-prototype-detail]'));
    await tick();
    assert.match(panel.textContent, /不调用业务API或伪报保存成功/);
    assert.match(panel.textContent, /gap-1/);
  }
  assert.match(h.el.textContent, /计划\/菜单可点≠检查通过/);
  assert.match(h.el.textContent, /实现并联调/);
  assert.equal(routes.calls(c => c.method === "POST").length, 0);
});

test("严格确认/非法JSON不写；原型保存只写权威计划，不verify/业务保存", async () => {
  const routes = routesFor();
  routes.post("/api/prototype/T-prototype", ({init}) => {
    const b = JSON.parse(init.body);
    return {...view(), applied: true, revision: 2, current: {operation_id: b.operation_id, plan: b.plan}};
  });
  const {h, form} = await ready(routes);
  fill(h, form, "confirm", " T-prototype");
  form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true})); await tick();
  assert.equal(routes.calls(c => c.method === "POST").length, 0);
  fill(h, form, "confirm", "T-prototype"); fill(h, form, "plan", "broken");
  form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true})); await tick();
  assert.equal(routes.calls(c => c.method === "POST").length, 0);
  fill(h, form, "plan", JSON.stringify(view().current.plan));
  form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true}));
  await waitFor(() => routes.calls(c => c.method === "POST").length === 1);
  assert.equal(routes.calls(c => c.method === "POST")[0].path, "/api/prototype/T-prototype");
});

test("未知回执单飞/保原ID；历史GET可核对被后续版本覆盖的原操作", async () => {
  const routes = routesFor(); const pending = new Deferred();
  routes.post("/api/prototype/T-prototype", pending.promise);
  const {h, form} = await ready(routes);
  form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true}));
  form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true}));
  await waitFor(() => routes.calls(c => c.method === "POST").length === 1);
  pending.reject(new Error("lost"));
  const button = await waitFor(() => h.el.querySelector('[data-act="prototyperecheck"]'));
  await tick();
  routes.get("/api/prototype/T-prototype", {...view(), revision: 3, applied_revision: 2});
  button.click();
  await waitFor(() => !h.el.querySelector('[data-act="prototyperecheck"]'));
  assert.equal(routes.calls(c => c.method === "POST").length, 1);
  assert.ok(routes.calls(c => c.method === "GET" && c.query.includes("operation_id=")).length);
});

test("损坏/不可读记录显示明确错误，不误当空清单或可写模板", async () => {
  const h = await boot(routesFor({ok: false, code: "PROTOTYPE_CORRUPT"}), {hash: "#/task/T-prototype"});
  h.window.dispatchEvent(new h.window.Event("hashchange"));
  await waitFor(() => h.el.textContent.includes("PROTOTYPE_CORRUPT"));
  assert.match(h.el.textContent, /不是空清单/);
  assert.equal(h.el.querySelector('form[data-form="prototype"]'), null);
});

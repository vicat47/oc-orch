import assert from "node:assert/strict";
import {test} from "node:test";
import {boot, baseRoutes, taskDetail, tick, waitFor} from "./_harness.mjs";

const TID = "T-1";

function routesFor(attemptStatus = "result_collected") {
  const routes = baseRoutes();
  routes.get(`/api/task/${TID}`, taskDetail({
    status: "running", current_attempt_id: "A-1",
    attempts: [{id: "A-1", generation: 1, status: attemptStatus,
                dispatch_state: "confirmed", session_id: "ses_w", verification: {}}],
  }));
  routes.post(`/api/task/${TID}/rework`, {
    ok: true, result: {task_id: TID, attempt_id: "A-2", generation: 2,
                       session_id: "ses_w"},
  });
  return routes;
}

async function ready(routes) {
  const h = await boot(routes, {hash: `#/task/${TID}`});
  await waitFor(() => h.el.querySelector('form[data-form="rework"]'));
  return h;
}

test("返工表单默认 fast，可切换 budget 并提交控制版本", async () => {
  const routes = routesFor();
  const h = await ready(routes);
  const form = h.el.querySelector('form[data-form="rework"]');
  assert.equal(form.querySelector('[name="work_mode"]').value, "latency-first");
  form.querySelector('[name="feedback"]').value = "修一点边界";
  form.querySelector('[name="work_mode"]').value = "budget-first";
  form.querySelector('[name="confirm"]').value = TID;
  form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true}));
  await waitFor(() => routes.calls(c => c.method === "POST").length === 1);
  const body = JSON.parse(routes.calls(c => c.method === "POST")[0].init.body);
  assert.equal(body.feedback, "修一点边界");
  assert.equal(body.work_mode, "budget-first");
  assert.equal(body.confirm, TID);
  await tick(100);
});

test("确认不匹配不 POST", async () => {
  const routes = routesFor();
  const h = await ready(routes);
  const form = h.el.querySelector('form[data-form="rework"]');
  form.querySelector('[name="feedback"]').value = "修一点边界";
  form.querySelector('[name="confirm"]').value = "wrong";
  form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true}));
  await waitFor(() => h.window.__dialogs.prompts.length >= 0);
  assert.equal(routes.calls(c => c.method === "POST").length, 0);
});

test("当前尝试非可返工状态时不显示表单", async () => {
  const routes = routesFor("result_received");
  const h = await boot(routes, {hash: `#/task/${TID}`});
  await waitFor(() => h.el.textContent.includes("任务操作"));
  assert.equal(h.el.querySelector('form[data-form="rework"]'), null);
});
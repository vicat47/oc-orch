import assert from "node:assert/strict";
import {test} from "node:test";
import {boot, baseRoutes, coord, waitFor, tick} from "./_harness.mjs";

async function ready(routes) {
  const h = await boot(routes, {hash: "#/coordinator/ses_user", prompt: message => message.includes("逐字") ? "p" : "补充说明"});
  h.window.localStorage.clear();
  h.window.dispatchEvent(new h.window.Event("hashchange"));
  await waitFor(() => h.el.querySelector('[data-act="usersend"]'));
  return h;
}
function routesFor() {
  const routes = baseRoutes();
  routes.get("/api/coordinator/ses_user", {ok: true, coordinator: coord("ses_user", {projects: ["p"]}), tasks: [], full_count: 0, after: null});
  routes.get("/api/coordinator/control", {ok: true, read_only: false, runtime: {delivery_policy_guard_valid: true},
    session_control: {revision: 0}, lease: {holder_session: "ses_user", epoch: 1}});
  return routes;
}

test("显式补充指令先落durable，unknown后重复点击不换ID/重投", async () => {
  const routes = routesFor();
  routes.post("/api/coordinator/message", () => {throw new Error("lost response");});
  const h = await ready(routes);
  h.el.querySelector('[data-act="usersend"]').click();
  await waitFor(() => routes.calls(c => c.method === "POST").length === 1);
  await tick();
  const saved = JSON.parse(h.window.localStorage.getItem("oc-orch.operator-turn.v1"));
  assert.equal(saved.length, 1); assert.equal(saved[0].state, "possible");
  h.el.querySelector('[data-act="usersend"]').click(); await tick();
  assert.equal(routes.calls(c => c.method === "POST").length, 1);
  assert.equal(JSON.parse(routes.calls(c => c.method === "POST")[0].init.body).operation_id, saved[0].body.operation_id);
});

test("本地持久意图损坏时零POST，不覆盖旧未决", async () => {
  const routes = routesFor(); const h = await ready(routes);
  h.window.localStorage.setItem("oc-orch.operator-turn.v1", "broken");
  h.el.querySelector('[data-act="usersend"]').click(); await tick();
  assert.equal(routes.calls(c => c.method === "POST").length, 0);
  assert.equal(h.window.localStorage.getItem("oc-orch.operator-turn.v1"), "broken");
});

test("明确4xx拒绝可保留refused并重新确认新操作；不把它当未知永久锁死", async () => {
  const routes = routesFor();
  routes.post("/api/coordinator/message", {status: 400, body: {ok: false, code: "OPERATOR_TURN_REVISION", message: "stale"}});
  const h = await ready(routes);
  h.el.querySelector('[data-act="usersend"]').click();
  await waitFor(() => JSON.parse(h.window.localStorage.getItem("oc-orch.operator-turn.v1") || "[]")[0]?.state === "refused");
  await tick();
  h.el.querySelector('[data-act="usersend"]').click();
  await waitFor(() => routes.calls(c => c.method === "POST").length === 2);
});

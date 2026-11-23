import assert from "node:assert/strict";
import {test} from "node:test";
import {boot, baseRoutes, coord, waitFor} from "./_harness.mjs";

const SID = "ses_user";
const P = "p";
const HOLD = {reason: "coordinator-interrupted", origin_at: 1000, held_at: 1000};

function routesFor({barrier = null, candidate = null} = {}) {
  const routes = baseRoutes();
  routes.get(`/api/coordinator/${SID}`, {
    ok: true,
    coordinator: coord(SID, {projects: [P]}),
    session_control: {
      ok: true, revision: 1, held: true, hold: HOLD,
      barrier, watermark: 1000, user_turn_active: barrier != null,
    },
    tasks: [], tasks_full_count: 0, next_after_id: null,
    tasks_truncated: false, notifications: [], responsibilities: [],
  });
  routes.get("/api/coordinator/control", {
    ok: true, read_only: false,
    session_control: {revision: 1, hold: HOLD, barrier},
    lease: {holder_session: SID, epoch: 1},
  });
  if (candidate) routes.get("/api/coordinator/turn-candidate", candidate);
  routes.post("/api/coordinator/register-turn", {
    ok: true, result: {executed: true, applied: true,
                       reason: "finished-with-hold-release"},
  });
  routes.post("/api/coordinator/release-hold", {
    ok: true, resurfaced: 1,
    result: {executed: true, applied: true, reason: "hold-released"},
  });
  return routes;
}

async function ready(routes, prompt, predicate) {
  const h = await boot(routes, {hash: `#/coordinator/${SID}`, prompt});
  h.window.dispatchEvent(new h.window.Event("hashchange"));
  await waitFor(() => predicate
    ? predicate(h)
    : h.el.querySelector('[data-act="checkturn"]'));
  return h;
}

const eligible = {
  ok: true, eligible: true, revision: 1,
  message_id: "msg_user_done", created: 1010,
  idle_id: "idle_done", idle_at: 1030, outcome: "succeeded",
  hold: HOLD,
};

test("回合 hold 可见；检查候选后登记已完成用户回合并带控制版本", async () => {
  const routes = routesFor({candidate: eligible});
  const h = await ready(routes, message =>
    message.includes("项目路径") ? P : null);
  assert.match(h.el.textContent, /回合控制/);
  assert.match(h.el.textContent, /coordinator-interrupted/);

  h.el.querySelector('[data-act="checkturn"]').click();
  await waitFor(() => h.el.querySelector('[data-act="registerturn"]'));
  h.el.querySelector('[data-act="registerturn"]').click();
  await waitFor(() => routes.calls(c => c.method === "POST").length === 1);
  const body = JSON.parse(routes.calls(c => c.method === "POST")[0].init.body);
  assert.equal(body.message_id, "msg_user_done");
  assert.equal(body.expected_revision, 1);
  assert.equal(body.expected_epoch, 1);
  assert.equal(body.confirm, P);
  assert.match(body.operation_id, /^web-register-turn-/);
});

test("运维强制解除需要项目确认与 RESUME，并发送当前版本", async () => {
  const routes = routesFor();
  const h = await ready(routes, message => {
    if (message.includes("解除原因")) return "web:test-resume";
    if (message.includes("项目路径")) return P;
    if (message.includes("RESUME")) return "RESUME";
    return null;
  });
  h.el.querySelector('[data-act="releasehold"]').click();
  await waitFor(() => routes.calls(c => c.method === "POST").length === 1);
  const body = JSON.parse(routes.calls(c => c.method === "POST")[0].init.body);
  assert.equal(body.reason, "web:test-resume");
  assert.equal(body.expected_revision, 1);
  assert.equal(body.expected_epoch, 1);
  assert.equal(body.confirm, P);
  assert.match(body.operation_id, /^web-release-hold-/);
});

test("有用户回合屏障时只展示，不显示解除/登记入口", async () => {
  const routes = routesFor({barrier: {message_id: "msg_turn", origin_at: 1010}});
  const h = await ready(routes, () => null,
    h => h.el.textContent.includes("用户回合屏障"));
  assert.equal(h.el.querySelector('[data-act="checkturn"]'), null);
  assert.equal(h.el.querySelector('[data-act="releasehold"]'), null);
  assert.match(h.el.textContent, /用户回合屏障/);
});

test("控制状态不可读时不显示写入口", async () => {
  const routes = routesFor();
  routes.get(`/api/coordinator/${SID}`, {
    ok: true, coordinator: coord(SID, {projects: [P]}),
    session_control: {ok: false, error: "CoordinatorControlStateError"},
    tasks: [], tasks_full_count: 0, next_after_id: null,
    tasks_truncated: false, notifications: [], responsibilities: [],
  });
  const h = await boot(routes, {hash: `#/coordinator/${SID}`, prompt: () => null});
  h.window.dispatchEvent(new h.window.Event("hashchange"));
  await waitFor(() => h.el.textContent.includes("回合控制"));
  assert.equal(h.el.querySelector('[data-act="releasehold"]'), null);
  assert.equal(h.el.querySelector('[data-act="checkturn"]'), null);
  assert.match(h.el.textContent, /回合控制不可读/);
});
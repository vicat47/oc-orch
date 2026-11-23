/**
 * 竞态与交互保全反例测试（父核心读审查的回归项）。
 *
 * 反例均以“期望行为”断言；核心补修落地前部分用例会红（作为明确待办信号），
 * 落地后应转绿——不用“自报通过数”替代真实反例：
 * - cache(keyed(...)) 必须真正缓存：诊断 details.open/checkbox → 全局任务 → 返回仍保留。
 * - 跨对象隔离：A 任务验收表单值不得残留到 B 任务。
 * - loadDetail 代次守卫：旧代响应（A 慢）不得覆盖新代（A 快）结果。
 * - verify 旧成功回执只应关闭自己启动的表单，不得删 close/reopen 后的新表单。
 * - send 提交成功后 reset 不得清空请求期间新输入。
 * - 批量 ACK 确认必须列出完整勾选集合（不只是前 8 条），并提交完整 ids。
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import {
  boot, baseRoutes, coord, snapshot, taskDetail, Deferred, waitFor, tick,
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

test("路由往返保留交互：诊断 details.open/checkbox → 全局任务 → 返回仍保留", async () => {
  const routes = baseRoutes();
  routes.get("/api/status", {ok: true, snapshot: snapshot({
    alerts: [{id: "al-1", severity: "error", kind: "global_fault", title: "全局故障",
              detail: "原文详情", state: "open"}],
    notifications: [{id: "N-1", kind: "test", state: "pending", summary: "s"}],
  })});
  routes.get("/api/coordinators", withCoords(coord("ses_a")));
  const h = await boot(routes);

  h.window.location.hash = "#/diagnostics";
  const details = await waitFor(() => h.document.querySelector("#diag-alerts details"));
  details.querySelector("summary").click();
  assert.equal(details.open, true);
  const cb = await waitFor(() =>
    h.document.querySelector('#diag-notif input[type="checkbox"].noteck'));
  cb.checked = false;
  cb.dispatchEvent(new Event("change", {bubbles: true}));

  h.window.location.hash = "#/tasks";
  await waitFor(() => h.document.querySelector("#taskSearch"));
  h.window.location.hash = "#/diagnostics";
  const details2 = await waitFor(() => h.document.querySelector("#diag-alerts details"));
  assert.equal(details2.open, true, "往返后已展开的 details 必须保留（缓存真实生效）");
  const noteck = await waitFor(() =>
    h.document.querySelector('#diag-notif input[type="checkbox"].noteck'));
  assert.equal(noteck.checked, false, "往返后取消勾选的 checkbox 必须保留");
});

test("跨对象隔离：任务 A 的验收表单值不残留到任务 B", async () => {
  const routes = baseRoutes();
  routes.get("/api/task/T-1", taskDetail({id: "T-1", title: "任务一"}));
  routes.get("/api/task/T-2", taskDetail({
    id: "T-2", title: "任务二", current_attempt_id: "A-2",
    attempts: [{id: "A-2", generation: 1, status: "result_received",
                dispatch_state: "confirmed", verification: {}}],
  }));
  const h = await boot(routes, {hash: "#/task/T-1"});

  await waitFor(() => h.el.textContent.includes("任务一"));
  h.el.querySelector('[data-act="verify"][data-id="A-1"]').click();
  const formA = await waitFor(() => h.document.querySelector('form[data-form="verify"]'));
  formA.querySelector('[name="evidence"]').value = "AAAA";

  h.window.location.hash = "#/task/T-2";
  await waitFor(() => h.el.textContent.includes("任务二"));
  assert.equal(h.document.querySelector('form[data-form="verify"]'), null,
               "A 的表单不得跨对象残留到 B");
  h.el.querySelector('[data-act="verify"][data-id="A-2"][data-pass="0"]').click();
  const formB = await waitFor(() => h.document.querySelector('form[data-form="verify"]'));
  assert.ok(formB.textContent.includes("验收 fail"), "B 显示 fail 表单");
  assert.equal(formB.querySelector('[name="evidence"]').value, "",
               "B 的表单不得带 A 的旧值");
});

test("详情代次守卫：旧代（A 慢）响应不覆盖新代（A 快）结果", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(coord("ses_a", {title: "协调者A"})));
  const h = await boot(routes);
  await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_a"]'));

  const slowA = new Deferred(); // 第一次进入 A（旧代）慢响应
  routes.get("/api/coordinator/ses_a", slowA.promise);
  h.window.location.hash = "#/coordinator/ses_a";
  await tick(40);

  routes.get("/api/task/T-1", taskDetail({title: "任务标题"}));
  h.window.location.hash = "#/task/T-1";
  await waitFor(() => h.el.textContent.includes("任务标题"));

  // 再次进入 A：应发起新代请求并拿到新数据（不复用旧代 pending）
  routes.get("/api/coordinator/ses_a", {status: 200, body: coordinatorDetail("ses_a", {
    coordinator: coord("ses_a", {title: "协调者A新"}),
    tasks: [{id: "T-N", title: "新任务", status: "running"}],
  })});
  h.window.location.hash = "#/coordinator/ses_a";
  await waitFor(() => h.el.textContent.includes("协调者A新"), {timeout: 3000});

  // 旧代此时才返回：不得覆盖新代结果
  slowA.resolve({status: 200, body: coordinatorDetail("ses_a", {
    coordinator: coord("ses_a", {title: "协调者A旧"}),
  })});
  await tick(60);
  assert.ok(h.el.textContent.includes("协调者A新"), "旧代响应不得覆盖新代视图");
  assert.ok(!h.el.textContent.includes("协调者A旧"), "不得展示旧代数据");
  assert.equal(h.state.details.get("coordinator:ses_a").body.coordinator.title,
               "协调者A新", "details 条目必须是新代数据");
});

test("verify 旧成功回执不得删除 close/reopen 后的新表单（同 attempt 不同 pass）", async () => {
  const routes = baseRoutes();
  routes.get("/api/task/T-1", taskDetail({status: "running"}));
  const slow = new Deferred();
  routes.post("/api/attempt/A-1/verify", slow.promise);
  const h = await boot(routes, {hash: "#/task/T-1"});

  await waitFor(() => h.el.textContent.includes("任务标题"));
  h.el.querySelector('[data-act="verify"][data-id="A-1"][data-pass="1"]').click();
  const form = await waitFor(() => h.document.querySelector('form[data-form="verify"]'));
  form.querySelector('[name="confirm"]').value = "A-1";
  form.dispatchEvent(new Event("submit", {bubbles: true, cancelable: true}));
  await tick(20);
  assert.equal(routes.count((c) => c.path === "/api/attempt/A-1/verify" && c.method === "POST"), 1);

  // 先关掉 pass 表单，再打开 fail 表单（同一 attempt、不同 pass）
  h.document.querySelector('[data-act="closeverify"]').click();
  await tick(20);
  assert.equal(h.document.querySelector('form[data-form="verify"]'), null, "已关闭");
  h.el.querySelector('[data-act="verify"][data-id="A-1"][data-pass="0"]').click();
  await waitFor(() => h.document.querySelector('form[data-form="verify"][data-pass="0"]'));

  // 旧 pass 成功回执返回：不得删除新 fail 表单
  slow.resolve({ok: true});
  await tick(80);
  const newForm = h.document.querySelector('form[data-form="verify"]');
  assert.ok(newForm, "旧回执不得关闭新表单");
  assert.equal(newForm.dataset.pass, "0", "新表单应是 fail");
});

test("send 提交成功后 reset 不得清空请求期间新输入", async () => {
  const routes = baseRoutes();
  const slowSend = new Deferred();
  routes.post("/api/input/send", slowSend.promise);
  const h = await boot(routes);
  h.window.location.hash = "#/diagnostics";
  const form = await waitFor(() => h.document.querySelector('form[data-form="send"]'));
  const textInput = form.querySelector('[name="text"]');
  form.querySelector('[name="session"]').value = "ses_w";
  form.querySelector('[name="intent"]').value = "execute";
  textInput.value = "go";
  form.querySelector('[name="owner"]').value = "ops";
  form.querySelector('[name="request_id"]').value = "r1";

  form.dispatchEvent(new Event("submit", {bubbles: true, cancelable: true}));
  await tick(20);
  assert.equal(routes.count((c) => c.path === "/api/input/send" && c.method === "POST"), 1);

  // 请求挂起期间用户继续输入
  textInput.value = "新文本";
  textInput.dispatchEvent(new Event("input", {bubbles: true}));
  slowSend.resolve({ok: true, out: {input: {input_id: "I-s1"}}});
  await tick(80);
  assert.equal(form.querySelector('[name="text"]').value, "新文本",
               "提交成功的 reset 不得清空请求期间新输入");
});

test("批量 ACK 确认展示完整勾选集合（不只前 8 条）并提交完整 ids", async () => {
  const notifications = Array.from({length: 12}, (_, i) => ({
    id: `N-${String(i).padStart(2, "0")}`, kind: "test", state: "pending", summary: `通知 ${i}`,
  }));
  const routes = baseRoutes();
  routes.get("/api/status", {ok: true, snapshot: snapshot({notifications})});
  routes.post("/api/notify/ack", {ok: true, result: {acked: 12}});
  const h = await boot(routes, {confirm: true});
  h.window.location.hash = "#/diagnostics";
  await waitFor(() => h.document.querySelector("#diag-notif .noteck"));
  assert.equal(h.document.querySelectorAll("#diag-notif .noteck").length, 12);

  h.document.querySelector('[data-act="ackchecked"]').click();
  await tick(30);
  const lastConfirm = h.dialogs.confirms.at(-1);
  for (let i = 0; i < 12; i++) {
    assert.ok(lastConfirm.includes(`N-${String(i).padStart(2, "0")}`),
              `确认框必须列出完整集合 N-${String(i).padStart(2, "0")}`);
  }
  const posts = routes.calls((c) => c.path === "/api/notify/ack" && c.method === "POST");
  assert.equal(posts.length, 1);
  assert.equal(JSON.parse(posts[0].init.body).ids.length, 12, "提交完整 ids");
});
test("coordinator 首次详情失败→点击重试→200 真正恢复（无缓存也须可恢复）", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinator/ses_a", {status: 500, body: {message: "boom"}});
  const h = await boot(routes, {hash: "#/coordinator/ses_a"});
  await waitFor(() => h.el.textContent.includes("加载失败"));
  assert.ok(h.el.querySelector('[data-act="manualrefresh"]'), "失败态提供重试按钮");

  routes.get("/api/coordinator/ses_a", {status: 200, body: coordinatorDetail("ses_a")});
  h.el.querySelector('[data-act="manualrefresh"]').click();
  await waitFor(() => h.el.textContent.includes("协调者ses_a"), {timeout: 4000});
  assert.ok(!h.el.textContent.includes("加载失败"), "重试成功后错误面板消失");
});

test("已载 2 页协调者列表导航离开再回：移走项剔除、不残留旧行", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinator/ses_a", {status: 200, body: coordinatorDetail("ses_a", {
    tasks: [{id: "T-1", title: "任务一", status: "running"}],
    tasks_full_count: 2, tasks_truncated: true, next_after_id: "T-2",
  })});
  routes.set("GET", "/api/coordinator/ses_a?after_id=T-2&limit=200", {status: 200, body: {
    ok: true, tasks: [{id: "T-2", title: "任务二", status: "running"}],
    tasks_truncated: false, next_after_id: null, tasks_full_count: 2,
  }});
  const h = await boot(routes, {hash: "#/coordinator/ses_a"});
  const moreBtn = await waitFor(() => h.el.querySelector('[data-act="coordmore"]'));
  moreBtn.click();
  await waitFor(() => h.el.textContent.includes("任务二"));
  assert.ok(h.el.textContent.includes("任务一"), "第 1 页任务已加载");

  // 服务端视角：T-1 已移走，只剩 T-2（不再截断）
  routes.get("/api/coordinator/ses_a", {status: 200, body: coordinatorDetail("ses_a", {
    tasks: [{id: "T-2", title: "任务二", status: "running"}],
    tasks_full_count: 1, tasks_truncated: false, next_after_id: null,
  })});
  h.window.location.hash = "#/tasks";
  await waitFor(() => h.document.querySelector("#taskSearch"));
  h.window.location.hash = "#/coordinator/ses_a";
  await waitFor(() => h.el.textContent.includes("协调者ses_a"));
  await h.refreshLight();
  await tick(40);
  assert.ok(h.el.textContent.includes("任务二"), "保留仍在的任务");
  assert.ok(!h.el.textContent.includes("任务一"), "移走项必须剔除，不残留");
});

test("DOM 断连重连后：订阅/事件/ticker 重新建立、数据继续更新且不重复绑定", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinator/ses_a", coordinatorDetail("ses_a", {
    notifications: [{id: "N-x", kind: "test", state: "pending", summary: "旧摘要"}],
  }));
  routes.post("/api/notify/ack", {ok: true, result: {acked: 1}});
  const h = await boot(routes, {hash: "#/coordinator/ses_a", confirm: true});
  const tab = await waitFor(() => h.el.querySelector('[data-tab="todos"]'));
  tab.click();
  await waitFor(() => h.el.querySelector('[data-act="acknote"]'));

  // 断连再重连：生命周期应重新建立（旧实现 _started 短路导致无订阅/ticker）
  h.el.remove();
  document.body.appendChild(h.el);
  await tick(50);

  routes.get("/api/coordinator/ses_a", coordinatorDetail("ses_a", {
    notifications: [{id: "N-x", kind: "test", state: "pending", summary: "新摘要"}],
  }));
  await h.refreshLight();
  await waitFor(() => h.el.textContent.includes("新摘要"), {timeout: 3000});
  const btn = await waitFor(() => h.el.querySelector('[data-act="acknote"]'));
  btn.click();
  await tick();
  assert.equal(routes.count((c) => c.path === "/api/notify/ack" && c.method === "POST"),
               1, "断连重连后仍只有一套事件，一次点击仅一次 POST");
});


test("导航 loadDetail pending 时点“继续加载”不发 after 请求；导航完成清 pending 且下一轮刷新请求详情", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(coord("ses_a", {title: "协调者A"})));
  routes.get("/api/coordinator/ses_a", {status: 200, body: coordinatorDetail("ses_a", {
    tasks: [{id: "T-1", title: "任务一", status: "running"}],
    tasks_full_count: 2, tasks_truncated: true, next_after_id: "T-2",
  })});
  routes.set("GET", "/api/coordinator/ses_a?after_id=T-2&limit=200", {status: 200, body: {
    ok: true, tasks: [{id: "T-2", title: "任务二", status: "running"}],
    tasks_truncated: false, next_after_id: null, tasks_full_count: 2,
  }});
  const h = await boot(routes, {hash: "#/coordinator/ses_a"});
  await waitFor(() => h.el.querySelector('[data-act="coordmore"]')); // 已缓存分页（pag 存在）

  // 离开再返回：导航 loadDetail 处于 pending（慢响应）
  h.window.location.hash = "#/overview";
  await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_a"]'));
  const slow = new Deferred();
  routes.get("/api/coordinator/ses_a", slow.promise);
  h.window.location.hash = "#/coordinator/ses_a";
  await tick(50);
  assert.ok(h.state.pendingRoute.has("coordinator:ses_a"), "导航详情应处于 pending");

  // 续页不得抢导航 token：pending 期间点击“继续加载”绝不能发出 after 请求
  h.el.querySelector('[data-act="coordmore"]').click();
  await tick(30);
  assert.equal(
    routes.count((c) => c.path === "/api/coordinator/ses_a"
                        && c.query === "?after_id=T-2&limit=200"),
    0, "导航 pending 时不得发续页 after 请求（避免抢 token 致 pendingRoute 永不清理）");

  // 导航完成后：pending 必须被清理，且下一轮刷新确实重新请求详情
  slow.resolve({status: 200, body: coordinatorDetail("ses_a", {
    tasks: [{id: "T-1", title: "任务一", status: "running"}],
    tasks_full_count: 2, tasks_truncated: true, next_after_id: "T-2",
  })});
  routes.get("/api/coordinator/ses_a", {status: 200, body: coordinatorDetail("ses_a", {
    tasks: [{id: "T-1", title: "任务一", status: "running"}],
    tasks_full_count: 2, tasks_truncated: true, next_after_id: "T-2",
  })});
  await waitFor(() => !h.state.pendingRoute.has("coordinator:ses_a"), {timeout: 4000});
  const before = routes.count((c) => c.path === "/api/coordinator/ses_a");
  await h.refreshLight();
  await tick(40);
  assert.ok(routes.count((c) => c.path === "/api/coordinator/ses_a") > before,
            "pending 清理后下一轮刷新必须实际请求详情（不再被永久跳过）");
  assert.equal(
    routes.count((c) => c.path === "/api/coordinator/ses_a"
                        && c.query === "?after_id=T-2&limit=200"),
    0, "全程不得发出续页 after 请求");
});

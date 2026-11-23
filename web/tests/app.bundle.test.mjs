/**
 * 打包产物（随 wheel/sdist 分发的 app.js）离线真实运行验证。
 *
 * 与源码 DOM 测试不同：本文件在全新 jsdom 中 import **打包字节**，验证的不是
 * 源码捷径。esbuild 必须把 views 静态内联进 bundle（变量 import() 不被打包则
 * 运行时 404），因此这里是“发布必须 build 离线 bundle 真跑”的守门测试。
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import {
  bootBundle, baseRoutes, coord, snapshot, waitFor, tick, packagedShell,
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

test("打包产物离线可运行：定义 oc-orch-app、渲染总览卡片、视图已内联非占位", async (t) => {
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(
    coord("ses_a", {title: "甲调度", attention: "关注甲"}),
    coord("ses_b", {title: "乙调度", attention: "关注乙"}),
  ));
  const h = await bootBundle(routes);
  t.after(() => h.dom.window.close());
  const card = await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_a"]'));
  assert.ok(card.textContent.includes("甲调度"));
  assert.ok(h.el.querySelector('[data-key="coordinator:ses_b"]'));
  assert.ok(!/尚未加载完成/.test(h.el.textContent), "视图必须已打包内联，不能是占位");
});

test("打包产物事件委托：todos tab + acknote 一次点击仅一次 POST", async (t) => {
  const routes = baseRoutes();
  routes.get("/api/coordinator/ses_a", coordinatorDetail("ses_a", {
    notifications: [{id: "N-b1", kind: "test", state: "pending", summary: "通知一"}],
  }));
  routes.post("/api/notify/ack", {ok: true, result: {acked: 1}});
  const h = await bootBundle(routes, {hash: "#/coordinator/ses_a", confirm: true});
  t.after(() => h.dom.window.close());
  const tab = await waitFor(() => h.el.querySelector('[data-tab="todos"]'));
  tab.click();
  const btn = await waitFor(() => h.el.querySelector('[data-act="acknote"]'));
  btn.click();
  await tick();
  const posts = routes.calls((c) => c.path === "/api/notify/ack" && c.method === "POST");
  assert.equal(posts.length, 1, "打包产物一次点击应恰好一次 POST");
  assert.deepEqual(JSON.parse(posts[0].init.body), {ids: ["N-b1"]});
});

test("打包产物 read_only：写按钮禁用，点击不发请求，读数据仍展示", async (t) => {
  const routes = baseRoutes();
  routes.get("/api/status", {ok: true, snapshot: snapshot({read_only: true})});
  routes.get("/api/coordinators", withCoords(coord("ses_a")));
  routes.get("/api/task/T-1", {
    ok: true,
    task: {id: "T-1", project: "p", title: "任务标题", status: "running",
           coordinator_source: "explicit", current_attempt_id: "A-1",
           effective_coordinator: "ses_c", coordinator_session: "ses_c"},
    attempts: [{id: "A-1", generation: 1, status: "result_received",
                dispatch_state: "confirmed", verification: {}}],
    attempts_full_count: 1, attempts_truncated: false, notifications: [],
  });
  const h = await bootBundle(routes, {hash: "#/task/T-1"});
  t.after(() => h.dom.window.close());
  await waitFor(() => h.el.textContent.includes("任务标题"));
  assert.match(h.el.textContent, /只读/, "read-only 徽标可见");
  for (const act of ["taskpause", "taskcancel", "verify", "collect"]) {
    assert.equal(h.el.querySelector(`[data-act="${act}"]`), null,
                 `read_only 下不得出现写动作 ${act}`);
  }
});

test("打包产物 XSS：注入标题按文本渲染且不执行", async (t) => {
  const payload = '<img src=x onerror="window.__pwned=1"><script>window.__pwned=2</script>';
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(coord("ses_evil", {title: payload})));
  const h = await bootBundle(routes);
  t.after(() => h.dom.window.close());
  await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_evil"]'));
  assert.equal(h.document.querySelectorAll("img").length, 0, "注入 img 不得实例化");
  assert.equal(h.document.querySelector("script"), null, "注入 script 不得实例化");
  assert.equal(h.window.__pwned, undefined, "注入代码不得执行");
});

test("完整打包页面外壳：应用加载后无 boot-note/初载提示残留，只有真实内容且 toast 唯一", async (t) => {
  const shell = packagedShell();
  assert.ok(!shell.includes('class="boot-note"'),
             "页壳不得含静态 class=boot-note 节点（防 light DOM 残留）");
  assert.ok(!shell.includes("正在加载 oc-orch 控制面"), "初载提示不得放在 HTML 静态内容中");
  const routes = baseRoutes();
  routes.get("/api/coordinators", {ok: true, meta: {}, coordinators: [
    coord("ses_a", {title: "甲调度"}),
    coord("ses_b", {title: "乙调度"}),
  ]});
  const h = await bootBundle(routes, {shell: true, hash: "#/"});
  t.after(() => h.dom.window.close());
  await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_a"]'));
  const text = h.document.body.textContent;
  assert.equal(h.document.querySelector(".boot-note"), null, "应用加载后不得残留 .boot-note");
  assert.ok(!text.includes("正在加载"), "不得残留初载提示“正在加载…”");
  assert.ok(text.includes("调度总览") && text.includes("甲调度"), "页面上是真实应用内容");
  assert.equal(h.document.querySelectorAll("#toast").length, 1, "完整外壳上 #toast 必须唯一");
});

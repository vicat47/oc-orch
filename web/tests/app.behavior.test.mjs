/**
 * oc-orch 应用真实 DOM 行为测试（jsdom + 源码模块共享实例）。
 *
 * 对应 openspec/specs/incremental-web：
 * - 列表按稳定 key 更新：节点身份保留、无周期加载占位、fresh=false 不误称在线。
 * - 交互保全：checkbox 勾选、details 展开、验收表单值/焦点/caret、搜索焦点。
 * - 数据刷新：失败保留旧数据并标过期、恢复清除；慢刷新单飞。
 * - 路由：切换后旧异步响应不覆盖当前视图；repeat 重渲染后一次点击仅一次 POST。
 * - 安全与可访问性：read_only 写控件禁用/隐藏且不发请求；危险操作需确认；
 *   注入按文本渲染不执行；诊断表格行合法；同毫秒多审计不丢行；分页续页去重。
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import {
  boot, Routes, baseRoutes, coord, snapshot, taskDetail, Deferred, waitFor, tick,
  packagedBundle, packagedShell, packagedCss, packagedManifest,
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

test("总览：按稳定 key 渲染，刷新更新数据不换节点、无加载占位", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(
    coord("ses_a", {title: "甲调度", attention: "关注甲", attention_rank: 3, created_at: 100}),
    coord("ses_b", {title: "乙调度", attention: "关注乙", attention_rank: 3, created_at: 200}),
  ));
  const h = await boot(routes);

  const cardA = await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_a"]'));
  assert.ok(cardA.textContent.includes("甲调度"));
  assert.ok(cardA.textContent.includes("关注甲"));
  assert.ok(h.el.querySelector('[data-key="coordinator:ses_b"]'));

  routes.get("/api/coordinators", withCoords(
    coord("ses_a", {title: "甲调度", attention: "关注甲（已更新）", attention_rank: 3, created_at: 100}),
    coord("ses_b", {title: "乙调度", attention: "关注乙", attention_rank: 3, created_at: 200}),
  ));
  await h.refreshLight();
  const cardA2 = await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_a"]'));
  assert.equal(cardA2, cardA, "刷新后同一业务对象必须复用同一 DOM 节点");
  assert.ok(cardA2.textContent.includes("关注甲（已更新）"));
  assert.ok(!/正在加载|加载中/.test(h.el.textContent), "刷新不显示周期性加载占位");
});

test("刷新保留 checkbox 取消勾选（自动刷新开关）", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(coord("ses_a"), coord("ses_b")));
  const h = await boot(routes);

  const checkbox = await waitFor(() => h.document.querySelector("#auto"));
  checkbox.checked = true;
  checkbox.dispatchEvent(new Event("change", {bubbles: true}));
  await h.refreshLight();
  assert.equal(h.document.querySelector("#auto").checked, true, "重新渲染不得重置用户勾选");
  checkbox.checked = false;
  checkbox.dispatchEvent(new Event("change", {bubbles: true}));
  await h.refreshLight();
  assert.equal(h.document.querySelector("#auto").checked, false,
               "后台刷新不得把用户取消的勾选重新置回");
});

test("刷新保留诊断 <details> 展开状态", async () => {
  const routes = baseRoutes();
  routes.get("/api/status", {ok: true, snapshot: snapshot({alerts: [{
    id: "al-1", severity: "error", kind: "global_fault", title: "全局故障",
    detail: "原文详情", state: "open",
  }]})});
  routes.get("/api/coordinators", withCoords(coord("ses_a")));
  const h = await boot(routes);
  h.window.location.hash = "#/diagnostics";
  const details = await waitFor(() => h.document.querySelector("#diag-alerts details"));
  details.querySelector("summary").click();
  assert.equal(details.open, true, "详情应可展开");
  await h.refreshLight();
  await tick();
  const details2 = h.document.querySelector("#diag-alerts details");
  assert.ok(details2, "刷新后 details 仍存在");
  assert.equal(details2.open, true, "刷新不得关闭已展开的 details");
});

test("验收表单：标准文案、值/焦点/caret 刷新保全、提交 force=false 且仅一次", async () => {
  const routes = baseRoutes();
  routes.get("/api/task/T-1", taskDetail({status: "running"}));
  routes.post("/api/attempt/A-1/verify", {ok: true});
  const h = await boot(routes, {hash: "#/task/T-1"});

  const verifyBtn = await waitFor(() =>
    h.el.querySelector('[data-act="verify"][data-id="A-1"]'));
  verifyBtn.click();
  const form = await waitFor(() => h.document.querySelector('form[data-form="verify"]'));
  // 标准验收文案：legend 显式 pass，提交按钮与说明不宣称人工 force
  assert.match(form.querySelector("legend").textContent, /通过验收.*A-1/);
  assert.match(form.textContent, /按标准验收/);
  assert.match(form.querySelector(".op-panel").textContent, /确认通过验收/);

  const evidence = form.querySelector('[name="evidence"]');
  form.querySelector('[name="confirm"]').value = "A-1";
  evidence.value = "证据一;证据二";
  evidence.setSelectionRange(3, 5);
  evidence.focus();
  await h.refreshLight();
  await tick();
  const form2 = h.document.querySelector('form[data-form="verify"]');
  assert.ok(form2, "刷新后表单仍在");
  assert.equal(form2.querySelector('[name="evidence"]').value, "证据一;证据二",
               "未提交表单的值必须保留");
  assert.equal(h.document.activeElement, form2.querySelector('[name="evidence"]'),
               "输入焦点必须保留");
  assert.deepEqual(
    [form2.querySelector('[name="evidence"]').selectionStart,
     form2.querySelector('[name="evidence"]').selectionEnd], [3, 5],
    "caret 选区必须保留");

  form2.querySelector('[name="notes"]').value = "备注内容";
  form2.dispatchEvent(new Event("submit", {bubbles: true, cancelable: true}));
  await tick();
  const posts = routes.calls((c) => c.path === "/api/attempt/A-1/verify" && c.method === "POST");
  assert.equal(posts.length, 1, "一次提交恰好一个 POST");
  const sent = JSON.parse(posts[0].init.body);
  assert.deepEqual(sent.evidence, ["证据一", "证据二"], "证据按分号切分");
  assert.equal(sent.confirm, "A-1");
  assert.equal(sent.pass, true);
  assert.equal(sent.force, false, "标准验收：不宣称人工例外");
  assert.equal(sent.run_checks, false);
});

test("搜索输入（#taskSearch）刷新保持焦点与内容", async () => {
  const routes = baseRoutes();
  routes.get("/api/status", {ok: true, snapshot: snapshot({
    tasks: [{id: "T-1", title: "任务标题", project: "p", status: "running",
             coordinator_session: "ses_c", last_progress_at: 1}],
  })});
  const h = await boot(routes, {hash: "#/tasks"});

  const search = await waitFor(() => h.document.querySelector("#taskSearch"));
  search.value = "任务";
  search.dispatchEvent(new Event("input", {bubbles: true}));
  search.focus();
  assert.equal(h.state.search, "任务", "输入同步到 state.search");
  await h.refreshLight();
  await tick();
  assert.equal(h.document.activeElement, search, "刷新期间搜索输入保持焦点");
  assert.equal(search.value, "任务", "搜索内容保留");
  assert.ok(h.el.textContent.includes("任务标题"));
});

test("失败保留旧数据并标过期，恢复后清除标记", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(coord("ses_a", {title: "甲"})));
  const h = await boot(routes);
  await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_a"]'));

  routes.get("/api/status", {status: 500, body: {message: "boom"}});
  await h.refreshLight();
  await tick();
  assert.equal(h.state.statusErr, true, "失败标志置起");
  assert.ok(h.el.querySelector('[data-key="coordinator:ses_a"]'), "失败必须保留已加载数据");
  assert.ok(h.el.textContent.includes("部分数据过期"), "失败必须页面可见");
  assert.match(h.el.textContent, /(?:状态|调度者|数据)[\s\S]{0,12}前/, "最后成功时间保留展示");

  routes.get("/api/status", {ok: true, snapshot: snapshot()});
  await h.refreshLight();
  await tick();
  assert.equal(h.state.statusErr, false, "恢复后错误标志清除");
  assert.ok(!h.el.textContent.includes("部分数据过期"), "恢复后失败标记消失");
});

test("慢刷新单飞：并发 refreshLight 复用同一轮请求", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(coord("ses_a")));
  const h = await boot(routes);
  await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_a"]'));

  const before = routes.count((c) => c.path === "/api/coordinators");
  const slow = new Deferred();
  routes.get("/api/coordinators", slow.promise);
  const p1 = h.refreshLight();
  await tick(30);
  const p2 = h.refreshLight();
  assert.equal(p2, p1, "单飞：进行中的刷新必须被复用，不启动新一轮");
  assert.equal(routes.count((c) => c.path === "/api/coordinators"), before + 1,
               "慢刷新期间不得叠加请求");
  slow.resolve({status: 200, body: withCoords(coord("ses_a"))});
  await p1;
  assert.equal(routes.count((c) => c.path === "/api/coordinators"), before + 1,
               "慢刷新完成后再无追加请求");
});

test("路由切换后旧异步响应不覆盖当前视图", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(coord("ses_a", {title: "协调者A"})));
  const h = await boot(routes);
  await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_a"]'));

  const slow = new Deferred();
  routes.get("/api/coordinator/ses_a", slow.promise);
  h.window.location.hash = "#/coordinator/ses_a";
  await tick(40); // hashchange 触发，详情请求已发出

  routes.get("/api/task/T-1", taskDetail({title: "任务标题"}));
  h.window.location.hash = "#/task/T-1";
  await waitFor(() => h.el.textContent.includes("任务标题"));

  slow.resolve({status: 200, body: coordinatorDetail("ses_a")});
  await tick(40);
  assert.ok(h.el.textContent.includes("任务标题"), "旧异步不得覆盖当前路由视图");
  assert.ok(!/调度者工作台/.test(h.el.textContent),
            "当前 task 视图不得渲染旧 coordinator 详情");
});

test("repeat 刷新后一次点击仅触发一次 POST（不重复绑定）", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinator/ses_a", coordinatorDetail("ses_a", {
    notifications: [{id: "N-1", kind: "test", state: "pending", summary: "通知一"}],
  }));
  routes.post("/api/notify/ack", {ok: true, result: {acked: 1}});
  const h = await boot(routes, {hash: "#/coordinator/ses_a", confirm: true});

  const todosTab = await waitFor(() => h.el.querySelector('[data-tab="todos"]'));
  todosTab.click();
  await waitFor(() => h.el.querySelector('[data-act="acknote"]'));
  await h.refreshLight(); // 刷新重渲染后事件仍单次绑定
  const btn = await waitFor(() => h.el.querySelector('[data-act="acknote"]'));
  btn.click();
  await tick();
  const posts = routes.calls((c) => c.path === "/api/notify/ack" && c.method === "POST");
  assert.equal(posts.length, 1, "一次点击应恰好一个 POST");
  assert.deepEqual(JSON.parse(posts[0].init.body), {ids: ["N-1"]});
});

test("read_only：写控件禁用或隐藏、不发请求，只读诊断仍展示", async () => {
  const routes = baseRoutes();
  routes.get("/api/status", {ok: true, snapshot: snapshot({read_only: true,
    notifications: [{id: "N-r", kind: "test", state: "pending"}]})});
  routes.get("/api/coordinators", withCoords(coord("ses_a")));
  routes.get("/api/task/T-1", taskDetail({status: "running"}));
  routes.get("/api/inputs?limit=50", {ok: true, inputs: [{
    input_id: "I-r1", session_id: "ses_r", intent: "execute", owner: "ops",
    admission: "delivered", delivery: "delivered", execution: "succeeded",
    business_ack: "acked", next_action: "close"}]});
  routes.post("/api/notify/ack", {ok: true});
  const h = await boot(routes, {hash: "#/task/T-1", confirm: true});

  await waitFor(() => h.el.textContent.includes("任务标题"));
  assert.match(h.el.textContent, /只读/, "read-only 徽标可见");
  for (const act of ["taskpause", "taskcancel", "verify", "collect", "taskresume"]) {
    assert.equal(h.el.querySelector(`[data-act="${act}"]`), null,
                 `read_only 任务视图不得出现写动作 ${act}`);
  }
  h.window.location.hash = "#/diagnostics";
  await waitFor(() => h.el.textContent.includes("I-r1"));
  assert.equal(h.document.querySelector("[data-managed]"), null,
               "read_only 受管输入区不渲染写按钮");
  const ackBtn = h.document.querySelector('[data-act="ackchecked"]');
  assert.ok(ackBtn && ackBtn.disabled, "read_only 批量 ACK 按钮禁用而非可用");
  ackBtn.click();
  await h.refreshLight();
  await tick();
  assert.equal(routes.count((c) => c.path === "/api/notify/ack" && c.method === "POST"),
               0, "read_only 点击写控件不得发出请求");
});

test("危险操作需确认：取消确认不发 POST，确认匹配仅发一次", async () => {
  const routes = baseRoutes();
  routes.get("/api/task/T-1", taskDetail({status: "running"}));
  routes.post("/api/task/T-1/cancel", {ok: true});

  const h = await boot(routes, {hash: "#/task/T-1", confirm: false});
  const cancelBtn = await waitFor(() => h.el.querySelector('[data-act="taskcancel"]'));
  cancelBtn.click();
  await tick();
  assert.equal(routes.count((c) => c.path === "/api/task/T-1/cancel"), 0,
               "确认被取消时不得发送写请求");

  const h2 = await boot(routes, {hash: "#/task/T-1", confirm: true,
                                  prompt: () => "T-1"});
  const cancelBtn2 = await waitFor(() => h2.el.querySelector('[data-act="taskcancel"]'));
  cancelBtn2.click();
  await tick();
  const posts = routes.calls((c) => c.path === "/api/task/T-1/cancel" && c.method === "POST");
  assert.equal(posts.length, 1, "确认匹配应恰好一次写请求");
  assert.equal(JSON.parse(posts[0].init.body).confirm, "T-1",
               "取消请求必须回填对象 ID 确认");
});

test("XSS：注入标题按文本渲染且不执行", async () => {
  const payload = '<img src=x onerror="window.__pwned=1"><script>window.__pwned=2</script>';
  const routes = baseRoutes();
  routes.get("/api/coordinators", withCoords(
    coord("ses_evil", {title: payload, attention: "关注"})));
  const h = await boot(routes);

  await waitFor(() => h.el.querySelector('[data-key="coordinator:ses_evil"]'));
  assert.equal(h.document.querySelectorAll("img").length, 0,
               "注入的 <img> 不得成为真实节点");
  assert.equal(h.document.querySelector("script"), null, "注入的 <script> 不得成为真实节点");
  assert.equal(h.window.__pwned, undefined, "注入的 onerror/脚本不得执行");
  assert.ok(h.el.textContent.includes("<img"), "注入内容按文本渲染（可见原样）");
});

test("审计：无 id 回退 key 唯一，同毫秒同实体不同动作不丢行", async () => {
  const routes = baseRoutes();
  routes.get("/api/audit?limit=60", {ok: true, audit: [
    {at: 1234567890, actor: "a", action: "verify", entity: "task", entity_id: "T-1",
     from_state: "pending", to_state: "accepted"},
    {at: 1234567890, actor: "a", action: "verify", entity: "task", entity_id: "T-1",
     from_state: "pending", to_state: "failed"},
    {at: 1234567890, actor: "b", action: "cancel", entity: "task", entity_id: "T-1",
     from_state: "running", to_state: "cancelled"},
  ]});
  const h = await boot(routes);
  h.window.location.hash = "#/diagnostics";
  const body = await waitFor(() => h.document.querySelector("#auditBody"));
  const rows = body.querySelectorAll("tr");
  assert.equal(rows.length, 3, "同毫秒同实体不同动作/行必须全部渲染");
});

test("诊断表格行合法：tbody 子元素为有效 tr，不出现非法 div/td 错位", async () => {
  const routes = baseRoutes();
  routes.get("/api/status", {ok: true, snapshot: snapshot({
    alerts: [{id: "al-x", severity: "error", kind: "x", title: "告警一", detail: "d"}],
    notifications: [{id: "N-t", kind: "test", state: "pending", summary: "s"}],
    leases: [], provider_backoffs: {},
  })});
  routes.get("/api/coordinators", withCoords(
    coord("ses_g", {gates: [{state: "manual-blocked", source: "manual"}],
                    leases: [{project: "/p", valid: true}]})));
  routes.get("/api/inputs?limit=50", {ok: true, inputs: [{
    input_id: "I-t1", session_id: "ses_t", intent: "execute", owner: "o",
    admission: "delivered", delivery: "delivered", execution: "succeeded",
    business_ack: "acked", next_action: "close"}]});
  routes.get("/api/responsibilities?limit=200", {ok: true, responsibilities: [{
    id: "R-t", state: "awaiting_review", task_id: "T-1", owner: "c",
    target_session: "t", wait_kind: "review", next_action: "x", revision: 1}]});
  const h = await boot(routes);
  h.window.location.hash = "#/diagnostics";
  const tbodyIds = ["#diag-gates tbody", "#diag-notif tbody", "#diag-alerts tbody",
                    "#managedBody", "#watchBody", "#respBody", "#auditBody"];
  for (const id of tbodyIds) {
    const tbody = await waitFor(() => h.document.querySelector(id));
    const kids = [...tbody.children];
    assert.ok(kids.length > 0, `${id} 应为非空表格`);
    for (const child of kids) {
      assert.equal(child.tagName, "TR", `${id} 直接子节点必须是 <tr>，发现 <${child.tagName}>`);
    }
  }
});

test("会话各自显示前台运行状态，刷新失败或离开快照时清除旧标记", async () => {
  const routes = baseRoutes();
  const a = coord("ses_a", {session_run_state: "running"});
  const b = coord("ses_b", {session_run_state: "inactive", counts: {running: 2}});
  const legacy = coord("ses_legacy", {online: true});
  routes.get("/api/coordinators", withCoords(a, b, legacy));
  const h = await boot(routes);
  const status = sid => h.el.querySelector(`[data-key="coordinator:${sid}"] .session-heading .session-run`)?.textContent;
  await waitFor(() => status("ses_a") === "运行中");
  assert.equal(status("ses_b"), "空闲");
  assert.equal(status("ses_legacy"), "状态未知");
  routes.get("/api/coordinators", {status: 500, body: {message: "offline"}});
  await h.refreshLight();
  await waitFor(() => status("ses_a") === "状态未知");
  routes.get("/api/coordinators", withCoords({...b, session_run_state: "running"}));
  await h.refreshLight();
  await waitFor(() => status("ses_b") === "运行中");
  assert.equal(status("ses_a"), "状态未知");
});

test("工作台标题展示当前会话状态，失败后的缓存不会标为运行中", async () => {
  const routes = baseRoutes();
  const path = "/api/coordinator/ses_work";
  const setState = value => routes.get(path, coordinatorDetail("ses_work", {
    coordinator: coord("ses_work", {session_run_state: value}),
  }));
  setState("running");
  const h = await boot(routes, {hash: "#/coordinator/ses_work"});
  const status = () => h.el.querySelector(".workspace-heading .session-run")?.textContent;
  await waitFor(() => status() === "运行中");
  routes.get(path, {status: 500, body: {message: "offline"}});
  await h.refreshLight();
  await waitFor(() => status() === "状态未知");
  setState("inactive");
  await h.refreshLight();
  await waitFor(() => status() === "空闲");
});

test("分页续页去重：锁定期间连续点击仅一次续页请求", async () => {
  const routes = baseRoutes();
  routes.get("/api/coordinator/ses_a", coordinatorDetail("ses_a", {
    tasks: [{id: "T-1", title: "任务一", status: "running"}],
    tasks_full_count: 2, tasks_truncated: true, next_after_id: "T-2",
  }));
  const h = await boot(routes, {hash: "#/coordinator/ses_a"});
  const moreBtn = await waitFor(() => h.el.querySelector('[data-act="coordmore"]'));
  const slow = new Deferred();
  routes.set("GET", "/api/coordinator/ses_a?after_id=T-2&limit=200", slow.promise);
  moreBtn.click();
  moreBtn.click(); // 第二次点击应被 pag.lock 去重
  await tick(30);
  assert.equal(
    routes.count((c) => c.path === "/api/coordinator/ses_a"
                        && c.query === "?after_id=T-2&limit=200"),
    1, "分页锁定期间不得重复续页请求");
  slow.resolve({status: 200, body: {
    ok: true, tasks: [{id: "T-2", title: "任务二", status: "running"}],
    tasks_truncated: false, next_after_id: null, tasks_full_count: 2,
  }});
  await tick(30);
  assert.ok(h.el.textContent.includes("任务二"), "续页结果应合并渲染");
});

test("打包外壳与 bundle 安全契约（本地资源、无 CDN、无 unsafeHTML）", () => {
  const shell = packagedShell();
  assert.match(shell, /oc-orch 控制面/);
  assert.doesNotMatch(shell, /<script(?: [^>]*)?>[^<]/, "不应存在内联脚本体");
  assert.doesNotMatch(shell, /src="http[^"]*"/, "无外部脚本 CDN 引用");
  assert.doesNotMatch(shell, /href="http[^"]*"/, "无外部样式 CDN 引用");
  assert.match(shell, /href="\/assets\/app\.css"/);
  assert.match(shell, /src="\/assets\/app\.js"/);
  const css = packagedCss();
  for (const needle of ["@media (max-width:768px)", "min-height:var(--touch)",
                        "--touch:44px", "prefers-reduced-motion", "body{overflow-x:hidden}"]) {
    assert.ok(css.includes(needle), `CSS 缺少 ${needle}`);
  }
  const bundle = packagedBundle();
  assert.doesNotMatch(bundle, /unsafeHTML/, "禁止 unsafeHTML 字符串式渲染");
  const manifest = packagedManifest();
  assert.equal(typeof manifest.version, "string");
  for (const file of ["app.js", "app.css", "index.html", "THIRD_PARTY_LICENSES.txt"]) {
    assert.ok(manifest.files[file]?.sha256, `manifest 缺少 ${file} 哈希`);
  }
  assert.ok(!("built_at" in manifest), "manifest 不得含时间戳（保持可复现）");
});
test("总览摘要与下钻范围一致，确认告警只提交当前列表中的 ID", async () => {
  const routes = baseRoutes();
  routes.get("/api/status", {ok: true, snapshot: snapshot({alerts: [
    {id: "G-1", title: "全局提醒"}, {id: "A-1", attempt_id: "attempt", title: "尝试提醒"},
  ]})});
  routes.get("/api/coordinators", withCoords(coord("ses_owner")));
  routes.get("/api/responsibilities?limit=200", {responsibilities: [
    {id: "R-other", owner: "operator"}, {id: "R-owned", owner: "ses_owner"},
  ]});
  routes.post("/api/alerts/ack", {ok: true});
  const h = await boot(routes, {confirm: true});
  const metric = section => h.el.querySelector(`.summary-band a[href="#/diagnostics/${section}"]`);
  await waitFor(() => metric("responsibilities")?.querySelector("strong").textContent === "1");
  assert.equal(metric("alerts").querySelector("strong").textContent, "1");
  metric("responsibilities").click();
  const rows = await waitFor(() => h.el.querySelector("#respBody"));
  assert.match(rows.textContent, /R-other/);
  assert.doesNotMatch(rows.textContent, /R-owned/);
  h.window.location.hash = "#/";
  await waitFor(() => metric("alerts"));
  metric("alerts").click();
  const ack = await waitFor(() => h.el.querySelector('[data-act="ackalerts"]'));
  assert.deepEqual([...h.el.querySelectorAll(".alck")].map(el => el.dataset.id), ["G-1"]);
  ack.click();
  await waitFor(() => routes.count(c => c.method === "POST") === 1);
  assert.deepEqual(JSON.parse(routes.calls(c => c.method === "POST")[0].init.body).ids, ["G-1"]);
});

test("页首显示调度进程与连接，重试和旧快照有明确标记", async () => {
  const routes = baseRoutes();
  const sup = {alive: true, healthy: true, connected: true, stream: {backoff: 0}};
  const setStatus = supervisor => routes.get("/api/status", {ok: true, snapshot: snapshot({supervisor})});
  setStatus(sup);
  const h = await boot(routes);
  const process = () => h.el.querySelector("#health summary .presence")?.textContent;
  const connection = () => [...h.el.querySelectorAll("#health dt")].find(dt => dt.textContent === "事件连接")?.nextElementSibling?.textContent;
  await waitFor(() => process() === "调度服务正常");
  assert.equal(connection(), "已连接");
  setStatus({...sup, stream: {backoff: 4}});
  await h.refreshLight();
  await waitFor(() => connection() === "正在重试");
  setStatus({...sup, alive: false, healthy: false});
  await h.refreshLight();
  await waitFor(() => process() === "调度服务未检测到进程");
  assert.equal(connection(), "未连接");
  setStatus({alive: true, healthy: false});
  await h.refreshLight();
  await waitFor(() => process() === "调度服务心跳异常");
  assert.equal(connection(), "未知");
  setStatus({healthy: true, connected: true});
  await h.refreshLight();
  await waitFor(() => connection() === "曾建立连接");
  routes.get("/api/status", {status: 500, body: {message: "offline"}});
  await h.refreshLight();
  await waitFor(() => process() === "调度服务待更新");
  assert.equal(connection(), "待更新");
});

// 当前发布bundle + 真Chromium + 临时loopback假API；不创建OpenCode session/费用。
import assert from "node:assert/strict";
import {createServer} from "node:http";
import {readFile, mkdir, writeFile} from "node:fs/promises";
import {fileURLToPath} from "node:url";
import path from "node:path";
import {chromium} from "playwright";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const assets = path.join(root, "src/oc_orch/entrypoints/web_assets");
const run = process.env.OC_ORCH_POLICY_BROWSER_RUN_DIR || "/tmp/opencode/oc-orch-delivery-policy-browser";
await mkdir(run, {recursive: true});
const taskId = "T-policy-browser", project = "p";
let revision = 1;
let policy = {preset: "budget-first", stage: "standard", limits: {max_parallel: 4, max_candidates: 2, max_challenge_rounds: 1}};
let operationId = "initial";
const writes = [];
const policyView = (scope = "task") => ({ok: true, schema: "oc-orch.delivery-policy.v1", scope,
  scope_id: scope === "task" ? taskId : project, project, managed: true, revision,
  version: {policy, operation_id: operationId}, execution_enforced: false,
  authorization: {revision: 0}, inflight: {items: [], total: 0}, instruction_target: null});
const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url, "http://127.0.0.1");
    const json = (value, status = 200) => { res.writeHead(status, {"Content-Type": "application/json"}); res.end(JSON.stringify(value)); };
    if (req.method === "POST") {
      let text = "";
      for await (const chunk of req) text += chunk;
      const body = JSON.parse(text);
      writes.push({path: url.pathname, body});
      if (url.pathname !== "/api/delivery-policy/task" || body.confirm !== taskId || body.expected_revision !== revision) {
        return json({ok: false, code: "DELIVERY_POLICY_REVISION_CONFLICT", message: "stale or wrong target"}, 400);
      }
      policy = body.policy;
      operationId = body.operation_id;
      revision++;
      return json({ok: true, applied: true, operation_id: operationId, applied_revision: revision, current: policyView(), execution_enforced: false});
    }
    if (url.pathname === "/" || url.pathname.startsWith("/assets/")) {
      const name = url.pathname === "/" ? "index.html" : url.pathname.slice(8);
      if (!["index.html", "app.js", "app.css"].includes(name)) { res.writeHead(404); return res.end(); }
      res.writeHead(200, {"Content-Type": name.endsWith("js") ? "text/javascript" : name.endsWith("css") ? "text/css" : "text/html"});
      return res.end(await readFile(path.join(assets, name)));
    }
    if (url.pathname === "/api/task/"+taskId) return json({ok: true,
      task: {id: taskId, project, title: "交付策略离线浏览器验收", status: "running", acceptance: ["不降低正式验收"]},
      attempts: [], delivery_policy: policyView(), project_delivery_policy: policyView("project")});
    if (url.pathname === "/api/status") return json({ok: true, snapshot: {read_only: false,
      supervisor: {healthy: true}, service: {}, tasks: [], notifications: [], alerts: [], counts: {}}});
    if (url.pathname === "/api/coordinators") return json({ok: true, coordinators: [], meta: {}});
    if (url.pathname === "/api/delivery-policy") return json(policyView());
    return json({ok: true, audit: [], inputs: [], watches: [], responsibilities: [], incidents: []});
  } catch (error) { res.writeHead(500); res.end(JSON.stringify({ok: false, message: error.message})); }
});
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const base = `http://127.0.0.1:${server.address().port}`;
let browser;
const results = [];
try {
  browser = await chromium.launch();
  for (const width of [360, 390, 768, 1280]) {
    const context = await browser.newContext({viewport: {width, height: 900}});
    const page = await context.newPage();
    const errors = [], external = [];
    page.on("pageerror", e => errors.push(e.message));
    await page.route("**/*", route => {
      if (!route.request().url().startsWith(base)) { external.push(route.request().url()); return route.abort(); }
      return route.continue();
    });
    await page.goto(base+"/#/task/"+taskId);
    await page.locator(".policy-panel").waitFor();
    assert.match(await page.locator(".policy-panel").textContent(), /运行时守卫尚未启用/);
    await page.locator(".policy-panel details").first().locator("summary").click();
    const form = page.locator('form[data-scope="task"]');
    await form.locator('[name="preset"]').selectOption("latency-first");
    await form.locator('[name="stage"]').selectOption("prototype");
    await form.locator('[name="reason"]').fill(`browser-${width}`);
    await form.locator('[name="confirm"]').fill(" "+taskId);
    await form.locator('button[type="submit"]').click();
    assert.equal(writes.length, results.length, "空白确认不得写入");
    await form.locator('[name="confirm"]').fill(taskId);
    const before = writes.length;
    await form.evaluate(el => {
      el.dispatchEvent(new Event("submit", {bubbles: true, cancelable: true}));
      el.dispatchEvent(new Event("submit", {bubbles: true, cancelable: true}));
    });
    await page.waitForFunction(() => document.querySelector("#toast")?.textContent.includes("策略已保存"));
    assert.equal(writes.length, before+1, "双提交只应有一次写入");
    assert.equal(writes.at(-1).body.policy.stage, "prototype");
    assert.equal(writes.at(-1).body.policy.preset, "latency-first");
    assert.match(await page.locator("#toast").textContent(), /尚未启用运行时守卫/);
    const size = await page.evaluate(() => ({client: document.documentElement.clientWidth, scroll: document.documentElement.scrollWidth}));
    assert.ok(size.scroll <= size.client+1, `viewport ${width} overflow ${JSON.stringify(size)}`);
    assert.deepEqual(errors, []);
    assert.deepEqual(external, []);
    await page.screenshot({path: path.join(run, `policy-${width}.png`), fullPage: true});
    results.push({width, passed: true, single_post: true, exact_confirmation: true, no_overflow: true,
      prototype_is_not_product_acceptance: true, runtime_enforced: false});
    await context.close();
  }
  const report = {schema: "oc-orch.delivery-policy-browser.v1", browser: browser.version(),
    source: "packaged-bundle + local-fake-api", real_opencode_sessions: false, production_changed: false, results};
  await writeFile(path.join(run, "validation.json"), JSON.stringify(report, null, 2)+"\n");
  console.log(JSON.stringify(report, null, 2));
} finally {
  await browser?.close();
  await new Promise(resolve => server.close(resolve));
}

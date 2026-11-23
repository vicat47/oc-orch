/**
 * 安全/转义证据（真实 DOM）：受管输入正文不展示、注入按文本渲染不执行。
 * 与 test_managed_input_web.py 的“服务端不得内插行数据”互为证据：服务端只发
 * 资源，客户端由 Lit 模板自动转义；正文字段从不渲染到 DOM。
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { boot, baseRoutes, snapshot, waitFor } from "./_harness.mjs";

test("诊断受管输入：正文不展示，注入 content 按文本渲染且不执行", async () => {
  const routes = baseRoutes();
  const payload = "<textarea>raw 正文泄露?</textarea><img src=x onerror=\"window.__pwn2=1\">";
  routes.get("/api/status", {ok: true, snapshot: snapshot()});
  routes.get("/api/inputs?limit=50", {ok: true, inputs: [{
    input_id: "I-x", session_id: "ses_x", intent: "execute", owner: "ops",
    admission: "delivered", delivery: "delivered", execution: "succeeded",
    business_ack: "acked", next_action: "close",
    text: payload,
  }]});
  const h = await boot(routes);
  h.window.location.hash = "#/diagnostics";
  await waitFor(() => h.el.textContent.includes("I-x"));
  const bodyText = h.document.body.textContent;
  assert.ok(!bodyText.includes("raw 正文泄露"), "受管输入正文不得展示");
  assert.equal(h.document.querySelectorAll("img").length, 0, "注入 img 不得实例化");
  assert.equal(h.document.querySelector("textarea"), null, "注入 textarea 不得实例化");
  assert.equal(h.window.__pwn2, undefined, "注入 onerror 不得执行");
});
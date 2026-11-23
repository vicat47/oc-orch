import assert from "node:assert/strict";
import {test} from "node:test";
import {boot, baseRoutes, taskDetail, waitFor, tick} from "./_harness.mjs";

test("晚到首帧不能从用户正在填写的证据框抢焦点；原证据完整提交", async () => {
  const routes = baseRoutes();
  routes.get("/api/task/T-1", taskDetail());
  routes.post("/api/attempt/A-1/verify", {ok: true});
  const h = await boot(routes, {hash: "#/task/T-1"});
  h.window.dispatchEvent(new h.window.Event("hashchange"));
  const button = await waitFor(() => h.el.querySelector('[data-act="verify"][data-pass="1"]'));
  const before = globalThis.requestAnimationFrame;
  let focusFirstFrame;
  globalThis.requestAnimationFrame = callback => {focusFirstFrame = callback; return 1;};
  try {
    button.click();
    const form = await waitFor(() => h.el.querySelector('form[data-form="verify"]'));
    const evidence = form.elements.evidence;
    evidence.focus(); evidence.value = "e1; e2; e3";
    evidence.dispatchEvent(new h.window.Event("input", {bubbles: true}));
    focusFirstFrame();
    assert.equal(h.window.document.activeElement, evidence);
    form.elements.confirm.value = "A-1";
    form.elements.confirm.dispatchEvent(new h.window.Event("input", {bubbles: true}));
    form.dispatchEvent(new h.window.Event("submit", {bubbles: true, cancelable: true}));
    await waitFor(() => routes.calls(c => c.method === "POST").length === 1);
    assert.deepEqual(JSON.parse(routes.calls(c => c.method === "POST")[0].init.body).evidence, ["e1", "e2", "e3"]);
    await tick();
  } finally {globalThis.requestAnimationFrame = before;}
});

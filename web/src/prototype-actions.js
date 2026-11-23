import {api, post} from "./api.js";
import {state, publish, ro} from "./state.js";
import {toast} from "./ui.js";
import {refreshLight} from "./data.js";
import {pendingPolicy, savePendingPolicy, clearPendingPolicy} from "./policy-storage.js";

export function rememberPrototypeInput(target) {
  const form = target?.closest?.('form[data-form="prototype"]');
  if (form?.dataset.task && target.name) state.prototypeDrafts.set(form.dataset.task,
    {...state.prototypeDrafts.get(form.dataset.task), [target.name]: target.value});
}

export async function submitPrototype(form) {
  const task = form.dataset.task, key = `prototype:${task}`;
  if (ro() || state.busyActions.has(key) || pendingPolicy(key)) return;
  const draft = state.prototypeDrafts.get(task) || {};
  const val = name => draft[name] ?? form.elements[name]?.value ?? "";
  if (val("confirm") !== task || !val("reason").trim()) { toast("须更新原因与逐字Task确认；未提交"); return; }
  let plan;
  try { plan = JSON.parse(val("plan")); } catch { toast("计划JSON非法，未提交"); return; }
  const revision = Number(form.dataset.revision);
  if (!Number.isSafeInteger(revision) || revision < 0) return;
  const body = {plan, reason: val("reason"), confirm: val("confirm"), expected_revision: revision,
    operation_id: `web-prototype-${crypto.randomUUID()}`};
  try { savePendingPolicy(key, {scope: "prototype", target: task, body}); }
  catch { toast("原意图持久化失败，零POST"); return; }
  state.busyActions.add(key); publish();
  try {
    const result = await post(`/api/prototype/${encodeURIComponent(task)}`, body);
    if (result.ok !== true || result.task_id !== task || result.current?.operation_id !== body.operation_id) throw new Error("原型回执身份未知");
    clearPendingPolicy(key);
    if (state.prototypeDrafts.get(task) === draft) state.prototypeDrafts.delete(task);
    toast(`已保存原型记录 v${result.revision}，未闭环 ${result.unresolved.length}；不是修复或正式验收`);
    await refreshLight();
  } catch (error) {
    if (error.status >= 400 && error.status < 500 && error.code !== "BAD_RESPONSE") clearPendingPolicy(key);
    toast(`${error.message}；没有自动重投，原操作 ${body.operation_id}`);
  } finally { state.busyActions.delete(key); publish(); }
}

export async function recheckPrototype(task) {
  const key = `prototype:${task}`, pending = pendingPolicy(key);
  if (!pending || pending.invalid) return;
  try {
    const out = await api(`/api/prototype/${encodeURIComponent(task)}?operation_id=${encodeURIComponent(pending.body.operation_id)}`);
    if (out.ok !== true || out.task_id !== task || !Number.isSafeInteger(out.applied_revision)) throw new Error("无可归属历史回执，不代表未执行");
    clearPendingPolicy(key);
    toast(`原操作应用 v${out.applied_revision}，当前 v${out.revision}；只读对账，无重投`);
    await refreshLight();
  } catch (error) { toast(error.message); }
  publish();
}

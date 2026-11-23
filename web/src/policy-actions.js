// 策略写请求只显式提交一次；未知回执保原operation/body，用GET对账而非换ID。
import {api, post} from "./api.js";
import {state, publish, ro} from "./state.js";
import {toast} from "./ui.js";
import {refreshLight} from "./data.js";
import {pendingPolicy, savePendingPolicy, clearPendingPolicy} from "./policy-storage.js";

export function rememberPolicyInput(target) {
  const form = target?.closest?.('form[data-form="delivery-policy"]');
  if (!form?.dataset.key || !target.name) return;
  const draft = {...state.policyDrafts.get(form.dataset.key)};
  draft[target.name] = target.type === "checkbox" ? target.checked : target.value;
  state.policyDrafts.set(form.dataset.key, draft);
}

export async function submitPolicy(form) {
  if (ro()) { toast("read-only：策略未提交"); return; }
  const {scope, target, key} = form.dataset;
  const busy = `policy:${key}`;
  if (state.busyActions.has(busy) || pendingPolicy(key)) return;
  if (!["task", "project"].includes(scope) || !target) return;
  const drafts = state.policyDrafts.get(key) || {};
  const val = name => drafts[name] ?? form.elements[name]?.value ?? "";
  if (val("confirm") !== target) { toast("确认文本必须逐字匹配目标"); return; }
  if (!String(val("reason")).trim()) { toast("切换原因必填"); return; }
  const expected_revision = Number(form.dataset.revision);
  if (!Number.isSafeInteger(expected_revision) || expected_revision < 0) return;
  const inherit = scope === "task" && (drafts.inherit ?? form.elements.inherit?.checked) === true;
  const limits = {};
  for (const [name, min, max] of [["max_parallel", 1, 32], ["max_candidates", 1, 8], ["max_challenge_rounds", 0, 8]]) {
    const raw = val(name);
    if (!/^\d+$/.test(String(raw))) { toast("策略上限必须是有限整数"); return; }
    const number = Number(raw);
    if (number < min || number > max) { toast(`策略上限 ${name} 超界`); return; }
    limits[name] = number;
  }
  const body = {[scope === "task" ? "task_id" : "project"]: target, expected_revision,
    operation_id: `web-policy-${crypto.randomUUID()}`, reason: val("reason"), confirm: val("confirm")};
  if (inherit) body.inherit = true;
  else body.policy = {preset: val("preset"), stage: val("stage"), limits};
  const operation = {body, scope, target};
  try { savePendingPolicy(key, operation); }
  catch { toast("无法保全原操作身份；策略未发送，请先恢复当前窗口存储"); return; }
  state.busyActions.add(busy);
  publish();
  try {
    const out = await post(`/api/delivery-policy/${scope}`, body);
    if (out.ok !== true || out.operation_id !== body.operation_id || out.current?.scope_id !== target
        || out.current?.scope !== scope || !Number.isSafeInteger(out.current?.revision)) {
      throw new Error("策略回执身份不完整，保原请求待只读核对");
    }
    clearPendingPolicy(key);
    // 提交后继续编辑的草稿不因旧回执被删除；保存结果不按按钮名称伪报。
    if (state.policyDrafts.get(key) === drafts) state.policyDrafts.delete(key);
    toast(`策略已保存：应用 v${out.applied_revision}，当前 v${out.current.revision}；${out.current.execution_enforced ? "仅影响后续安全决策" : "尚未启用运行时守卫"}`);
    await refreshLight();
  } catch (error) {
    // 只有明确4xx业务拒绝可视为未执行；5xx/坏JSON/网络结果未知保留原体。
    if (error.status >= 400 && error.status < 500 && error.code !== "BAD_RESPONSE") clearPendingPolicy(key);
    toast(`${error.message || "策略提交失败"}；未自动重投。原操作 ${body.operation_id}`);
  } finally {
    state.busyActions.delete(busy);
    publish();
  }
}

export async function recheckPolicy(scope, target) {
  const key = `${scope}:${target}`;
  const pending = pendingPolicy(key);
  if (!pending || state.busyActions.has(`policy:${key}`)) return;
  try {
    const query = scope === "task" ? "task_id" : "project";
    const current = await api(`/api/delivery-policy?${query}=${encodeURIComponent(target)}`);
    if (current.scope !== scope || current.scope_id !== target) throw new Error("只读对账身份不符");
    if (current.version?.operation_id === pending.body.operation_id) {
      clearPendingPolicy(key);
      toast(`原操作已在台账确认，当前 v${current.revision}；没有重投`);
    } else toast(`原操作尚未能由当前head确认（可能已有后续切换）。保留 ${pending.body.operation_id}，请用CLI审计核对，禁止猜测未执行。`);
    await refreshLight();
  } catch (error) { toast(`只读对账失败：${error.message}；原操作保留`); }
  publish();
}

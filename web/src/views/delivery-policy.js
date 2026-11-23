// 显式交付策略：只投影服务端事实；预设、费用授权、验收互不替代。
import {html, nothing} from "lit";
import {state, ro} from "../state.js";
import {ts} from "../ui.js";
import {pendingPolicy} from "../policy-storage.js";

const modes = [["budget-first", "预算优先"], ["elastic", "弹性加速"], ["latency-first", "时效优先"]];

function policyForm(view, scope, target) {
  const key = `${scope}:${target}`;
  const draft = state.policyDrafts.get(key) || {};
  const p = view.version?.policy || {preset: "budget-first", stage: "standard", limits: {max_parallel: 4, max_candidates: 2, max_challenge_rounds: 1}};
  const pending = pendingPolicy(key);
  const value = (name, fallback) => draft[name] ?? fallback;
  if (ro() || view.ok !== true || !Number.isSafeInteger(view.revision)) return nothing;
  return html`<details><summary>${scope === "task" ? "调整任务策略" : "设置新任务的项目默认策略"}</summary>
    <form data-form="delivery-policy" data-key=${key} data-scope=${scope} data-target=${target} data-revision=${view.revision}>
      <label>资源偏好 <select name="preset" .value=${value("preset", p.preset)}>${modes.map(([id, label]) => html`<option value=${id} ?selected=${value("preset", p.preset) === id}>${label}</option>`)}</select></label>
      <label>工作阶段 <select name="stage" .value=${value("stage", p.stage)}><option value="standard" ?selected=${value("stage", p.stage) === "standard"}>正式交付</option><option value="prototype" ?selected=${value("stage", p.stage) === "prototype"}>快速原型</option></select></label>
      ${[["max_parallel", "分工并发", 1, 32], ["max_candidates", "完整候选", 1, 8], ["max_challenge_rounds", "可选挑战轮次", 0, 8]].map(([name, label, min, max]) => html`
        <label>${label} <input type="number" name=${name} min=${min} max=${max} step="1" .value=${String(value(name, p.limits[name]))} required></label>`)}
      ${scope === "task" ? html`<label><input type="checkbox" name="inherit" ?checked=${draft.inherit === true}>改为继承当前项目默认快照</label>` : nothing}
      <label>切换原因 <input name="reason" .value=${value("reason", "")} required></label>
      <label>逐字回填 ${target} <input name="confirm" .value=${value("confirm", "")} autocomplete="off" required></label>
      <div class="note">版本 ${view.revision}。新策略用于后续工作，费用需单独授权。</div>
      ${pending ? html`<div class="warn">请求结果待对账：<code>${pending.body.operation_id}</code>。不自动重投，也不创建新操作绕过。</div><button type="button" data-act="policyrecheck" data-scope=${scope} data-target=${target}>只读核对原请求</button>` : html`<button type="submit" class="primary" ?disabled=${state.busyActions.has(`policy:${key}`)}>保存策略</button>`}
    </form></details>`;
}

export function renderDeliveryPolicy(task, view, projectView) {
  if (!view) return html`<h3>交付策略</h3><div class="muted">当前服务未提供策略信息。</div>`;
  if (view.ok !== true) return html`<h3>交付策略</h3><div class="bad">策略不可读：${view.code || "unknown"}，暂时无法调整。</div>`;
  const p = view.version?.policy;
  const inflight = view.inflight || {items: [], total: 0};
  return html`<section class="policy-panel"><h3>交付策略</h3>
    <div>${view.managed ? `${modes.find(([id]) => id === p?.preset)?.[1] || p?.preset} / ${p?.stage === "prototype" ? "快速原型" : "正式交付"} / v${view.revision}` : "未配置任务策略"}</div>
    <div class=${view.execution_enforced ? "note" : "warn"}>${view.execution_enforced ? "后续受管输入已接许可守卫；不是全链费用保证" : "策略草案 · 运行时守卫尚未启用"}</div>
    <div class="note">来源：${view.version?.source ? `项目默认 v${view.version.source.revision}` : "任务显式策略或未配置"}。独立资源授权 v${view.authorization?.revision ?? 0}；授权明细/撤销走独立 operator 入口。</div>
    <div class="note">共用协调者费用独立。任务仍按既定标准验收。</div>
    ${view.routing_context ? html`<details><summary>模型路线与上下文记录</summary>
      <div>${view.routing_context.continuity_reason}</div><div>缺失/过期是 unknown，不是零开销或缓存命中，不保证换路更快。</div>
      <ul>${Object.entries(view.routing_context.observations || {}).map(([name, item]) => html`<li>${name}：${item.state} / ${item.value == null ? "unknown" : String(item.value)} / 来源 ${item.source || "unknown"} / ${ts(item.observed_at)}</li>`)}</ul></details>` : nothing}
    ${inflight.total ? html`<div class="warn">仍有 ${inflight.total} 项可能在途（${inflight.truncated ? "列表截断" : "含原ID未知"}，不承诺撤回/退费）：</div><ul>${(inflight.items || []).map(item => html`<li>${item.attempt_id || item.input_id} / ${item.message_id} / v${item.policy_revision} / ${item.model} / ${item.cost} / ${ts(item.issued_at)}${item.older_policy ? " · 使用旧策略" : ""}</li>`)}</ul>` : html`<div class="muted">当前无已记录在途许可；这不证明未纳管路线或内部调用已停止。</div>`}
    ${policyForm(view, "task", task.id)}
    ${projectView ? policyForm(projectView, "project", task.project) : nothing}
  </section>`;
}

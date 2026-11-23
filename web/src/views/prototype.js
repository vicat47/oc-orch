// 原型范围/菜单与权威缺口。只展示计划事实，不运行startup/checks，不推导已联调。
import {html, nothing} from "lit";
import {state, ro} from "../state.js";
import {pendingPolicy} from "../policy-storage.js";

export const emptyPlan = () => ({question: "本轮要验证的问题", entry: "本轮体验入口", startup: "隔离启动方法（仅展示，不执行）",
  checks: ["需要注册并实际运行的检查"], paths: [{id: "main", features: ["pending"]}],
  features: [{id: "pending", title: "待实现菜单", state: "placeholder", source: "未接后端，零业务副作用", gap_ids: ["gap-1"]}],
  gaps: [{id: "gap-1", kind: "tradeoff", state: "open", risk: "ordinary", expected: "真实业务接入", current: "占位",
    reason: "本轮先验证交互", impact: "不可正式使用", owner: "operator", next_action: "实现后注册联调检查",
    review_at: "待定回访时间", refs: ["本Task"], followup_task: null, evidence: null}]});
const labels = {real: "真实接入", mock: "mock（仅模拟，无真实保存）", placeholder: "占位（未实现）", blocked: "受阻（不可用）"};

export function renderPrototype(task, view) {
  if (!view) return nothing;
  if (view.ok !== true) return html`<section><h3>原型范围与缺口</h3><div class="bad">权威记录不可读：${view.code || "unknown"}；不是空清单，不放行编辑。</div></section>`;
  const plan = view.current?.plan, key = `prototype:${task.id}`;
  const draft = state.prototypeDrafts.get(task.id) || {};
  const pending = pendingPolicy(key);
  const selected = plan?.features.find(f => f.id === state.prototypeSelected.get(task.id));
  return html`<section class="policy-panel"><h3>原型范围与缺口 / v${view.revision}</h3>
    <div class="warn">仅验证本轮声明范围；计划/菜单可点≠检查通过，登记≠修复，不降低原Task正式验收。</div>
    ${plan ? html`<dl class="kv"><dt>验证问题</dt><dd>${plan.question}</dd><dt>体验入口</dt><dd>${plan.entry}</dd>
      <dt>启动方法（仅文本）</dt><dd><code>${plan.startup}</code></dd><dt>声明检查（须另行实跑）</dt><dd>${plan.checks.join("；")}</dd>
      <dt>代表路径</dt><dd>${plan.paths.map(p => html`<div>${p.id}：${p.features.join(" → ")}</div>`)}</dd></dl>
      <div class="op-panel">${plan.features.map(f => html`<button data-act="prototypefeature" data-task=${task.id} data-feature=${f.id}>${f.title} · ${labels[f.state] || "unknown"}</button>`)}</div>
      ${selected ? html`<div role="status" data-prototype-detail class="attn rank-mid"><strong>${selected.title}：${labels[selected.state]}</strong>
        <div>数据/副作用边界：${selected.source}。此处只查看声明，不调用业务API或伪报保存成功。</div><div>权威缺口：${selected.gap_ids.join("、") || "无延期关联；真实验收仍走collect/verify"}</div></div>` : nothing}
      <div>未闭环 ${view.unresolved?.length ?? "unknown"} 项；记账不等于修复。</div>
      ${plan.gaps.map(g => html`<details><summary>${g.id} / ${g.kind} / ${g.state} / 风险 ${g.risk}</summary>
        <dl class="kv"><dt>期望/现状</dt><dd>${g.expected} / ${g.current}</dd><dt>原因/影响</dt><dd>${g.reason} / ${g.impact}</dd>
          <dt>责任/下一动作</dt><dd>${g.owner} / ${g.next_action}</dd><dt>回访</dt><dd>${g.review_at}</dd><dt>引用</dt><dd>${g.refs.join("；")}</dd>
          <dt>后续Task/修复证据</dt><dd>${g.followup_task || "原Task"} / ${g.evidence ? `${g.evidence.attempt_id}@${g.evidence.verified_at}` : "未修复，无正式证据"}</dd></dl></details>`)}
      <details><summary>保留的原Task承诺</summary><pre>${JSON.stringify(view.commitment, null, 2)}</pre></details>` : html`<div class="muted">尚无原型计划；不代表无缺口或原型已验收。</div>`}
    ${!ro() && !view.read_only ? html`<details><summary>显式更新原型计划与缺口（不改Task验收）</summary>
      <form data-form="prototype" data-task=${task.id} data-revision=${view.revision}>
        <label>计划JSON <textarea name="plan" rows="12" .value=${draft.plan ?? JSON.stringify(plan || emptyPlan(), null, 2)}></textarea></label>
        <label>更新原因 <input name="reason" .value=${draft.reason ?? ""}></label><label>逐字确认Task <input name="confirm" placeholder=${task.id} .value=${draft.confirm ?? ""}></label>
        ${pending ? html`<div class="warn">原操作待对账：${pending.body.operation_id}；不换ID重投。</div>
          <button type="button" data-act="prototyperecheck" data-task=${task.id}>只读核对原记录</button>`
          : html`<button type="submit" ?disabled=${state.busyActions.has(key)}>保存声明与责任（不是完成验收）</button>`}
      </form></details>` : nothing}</section>`;
}

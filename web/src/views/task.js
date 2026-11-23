// 任务详情：目标/验收标准/当前尝试/操作/验收表单/历史尝试。纯 Lit html 模板。
// 写动作经 data-act（taskpause/taskresume/taskcancel/collect/verify）冒泡，由 app 唯一委托。
import {html, nothing} from "lit";
import {repeat} from "lit/directives/repeat.js";
import {ro} from "../state.js";
import {ts, badge, copyButton} from "../ui.js";
import {renderReworkForm, renderVerifyForm} from "./forms.js";
import {renderDeliveryPolicy} from "./delivery-policy.js";
import {renderPrototype} from "./prototype.js";

export function renderTask(id, data = {}) {
  const t = data.task || {};
  const attempts = data.attempts || [];
  const cur = attempts.find(a => a.id === t.current_attempt_id);
  const ver = cur && cur.verification ? cur.verification : {};
  // 有效协调者 = 显式绑定优先，否则项目租约持有者（由 /api/task/<id> 计算返回）
  const eff = t.effective_coordinator;
  const src = t.coordinator_source; // "explicit" | "lease" | null
  const coordinator = eff
    ? html`<a href=${`#/coordinator/${encodeURIComponent(eff)}`}><code class="mono">${eff}</code></a><div class="muted">${src === "lease" ? "项目租约归属（未显式绑定到本项目租约持有者）" : "显式绑定"}</div>`
    : html`<span class="warn">未绑定</span><div class="muted">无显式 coordinator_session 且无项目租约持有者</div>`;
  const acts = [];
  if (!ro()) {
    if (t.status === "paused") acts.push(html`<button data-act="taskresume" data-id=${t.id}>resume</button>`);
    else if (!["accepted", "cancelled"].includes(t.status)) acts.push(html`<button data-act="taskpause" data-id=${t.id}>pause</button>`);
    if (!["accepted", "cancelled"].includes(t.status)) acts.push(html`<button class="danger" data-act="taskcancel" data-id=${t.id} data-title=${t.title || t.id}>取消任务</button>`);
    if (cur && ["result_pending", "result_received", "result_collected", "result_missing"].includes(cur.status)) {
      acts.push(html`<button class="primary" data-act="verify" data-id=${cur.id} data-pass="1">verify pass</button>`);
      acts.push(html`<button data-act="verify" data-id=${cur.id} data-pass="0">verify fail</button>`);
    }
    if (cur) acts.push(html`<button data-act="collect" data-id=${cur.id}>collect</button>`);
  }
  const canRework = !ro() && cur && !["paused", "cancelled", "accepted"].includes(t.status)
    && ["terminal_succeeded", "terminal_failed", "terminal_interrupted",
        "result_collected", "result_missing", "verification_failed", "rejected"].includes(cur.status);
  const historyNote = data.attempts_truncated ? `，仅最近 ${attempts.length} 条` : "";
  return html`<div class="breadcrumb"><a href="#/">总览</a> <span class="sep">/</span> <a href="#/tasks">全局任务</a> <span class="sep">/</span> <span>${t.title || t.id}</span></div>
    <div class="row"><h2>${t.title || t.id}</h2>${badge(t.status)}<span class="spacer"></span>${copyButton(t.id)}</div>
    <dl class="kv">
      <dt>任务 ID</dt><dd><code class="mono">${t.id}</code></dd>
      <dt>项目</dt><dd>${t.project || "-"}</dd>
      <dt>协调者</dt><dd>${coordinator}</dd>
      <dt>创建</dt><dd>${ts(t.created_at)}</dd>
      <dt>最近更新</dt><dd>${ts(t.updated_at)}</dd>
      <dt>已验收/关闭</dt><dd>${ts(t.accepted_at)} / ${ts(t.closed_at)}</dd>
      <dt>期限 / 下次检查</dt><dd>${ts(t.deadline_at)} / ${ts(t.next_check_at)}</dd>
    </dl>
    ${t.blocked_reason ? html`<div class="attn rank-mid"><span>受阻原因：${t.blocked_reason}</span></div>` : nothing}
    <h3>目标</h3><pre>${t.objective || "-"}</pre>
    <h3>验收标准</h3><pre>${t.acceptance || "-"}</pre>
    ${renderDeliveryPolicy(t, data.delivery_policy, data.project_delivery_policy)}
    ${renderPrototype(t, data.prototype)}
    <h3>当前尝试${cur ? html`（g${cur.generation}）` : nothing}</h3>
    ${cur ? html`<dl class="kv">
      <dt>Attempt</dt><dd><a href=${`#/attempt/${encodeURIComponent(cur.id)}`}><code class="mono">${cur.id}</code></a></dd>
      <dt>状态</dt><dd>${badge(cur.status)} / dispatch ${badge(cur.dispatch_state || "-")}</dd>
      <dt>执行 session</dt><dd><code class="mono">${cur.session_id || "-"}</code></dd>
      <dt>创建/最近进展</dt><dd>${ts(cur.created_at)} / ${ts(cur.last_progress_at)}</dd>
      ${ver.result ? html`<dt>验收</dt><dd>${badge(ver.result)} （${ver.classification || "-"}，${ts(ver.verified_at)}，actor=${ver.actor || "-"}）</dd>` : nothing}
      ${ver.missing && ver.missing.length ? html`<dt>未满足</dt><dd class="bad">${ver.missing.join("、")}</dd>` : nothing}
    </dl><div class="note">执行与证据详情见 <a href=${`#/attempt/${encodeURIComponent(cur.id)}`}>尝试详情</a>。</div>` : html`<div class="empty">当前无尝试。</div>`}
    <h3>任务操作${ro() ? "（read-only 不可写）" : ""}</h3>
    <div class="op-panel">${acts.length ? acts : html`<span class="muted">当前状态无可执行操作${ro() ? " / read-only" : ""}</span>`}</div>
    <div id="taskFormHost">${renderVerifyForm("task:" + id)}</div>
    ${canRework ? renderReworkForm(t.id) : nothing}
    <h3>历史尝试（${data.attempts_full_count ?? 0}${historyNote}）</h3>
    <div class="tablewrap"><table><thead><tr><th>Attempt</th><th>g</th><th>状态</th><th>结果路径</th><th>验收</th></tr></thead><tbody>
      ${attempts.length ? repeat(attempts, a => a.id, a => html`<tr><td><a href=${`#/attempt/${encodeURIComponent(a.id)}`}><code class="mono">${a.id}</code></a></td><td>${a.generation}</td><td>${badge(a.status)}</td><td class="mono">${a.result_path || "-"}</td><td>${a.verification && a.verification.result ? html`${badge(a.verification.result)} ${a.verification.classification || ""}` : html`<span class="muted">-</span>`}</td></tr>`) : html`<tr><td colspan="5" class="muted">(none)</td></tr>`}
    </tbody></table></div>`;
}

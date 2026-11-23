// 尝试详情：执行/派单/停止现场、验收结论、结果摘要、交付清单。纯 Lit html 模板。
// 写动作经 data-act（collect/verify）冒泡，由 app 唯一委托。
import {html, nothing} from "lit";
import {repeat} from "lit/directives/repeat.js";
import {ro} from "../state.js";
import {ts, badge, copyButton} from "../ui.js";
import {renderVerifyForm} from "./forms.js";

export function renderAttempt(id, data = {}) {
  const a = data.attempt || {};
  const ver = a.verification || {};
  const acts = [];
  if (!ro()) {
    if (["result_pending", "result_received", "result_collected", "result_missing"].includes(a.status)) {
      acts.push(html`<button class="primary" data-act="verify" data-id=${a.id} data-pass="1">verify pass</button>`);
      acts.push(html`<button data-act="verify" data-id=${a.id} data-pass="0">verify fail</button>`);
    }
    if (a.id) acts.push(html`<button data-act="collect" data-id=${a.id}>collect</button>`);
  }
  const dels = data.deliverables || [];
  const verdict = ver && ver.result
    ? html`<div class="attn ${ver.result === "pass" ? "" : "rank-high"}" style=${ver.result === "pass" ? "border-color:#2ea04388" : "border-color:#f8514955"}><span>${badge(ver.result)} ${ver.classification || ""} · ${ts(ver.verified_at)} · actor=${ver.actor || "-"}${ver.force_reason ? html` · force：${ver.force_reason}` : nothing}${ver.reason ? html` · 原因：${ver.reason}` : nothing}</span>${ver.missing && ver.missing.length ? html`<div class="note bad">未满足：${ver.missing.join("、")}${(ver.unmet || []).length ? `；未满足项 ${JSON.stringify(ver.unmet)}` : ""}</div>` : nothing}${ver.evidence && ver.evidence.length ? html`<div class="note">证据：<code class="mono">${ver.evidence.join("；")}</code></div>` : nothing}</div>`
    : html`<div class="note">尚无验收结论。任务只按此结论显示 accepted，通知 ACK 不算验收。</div>`;
  return html`<div class="breadcrumb"><a href="#/">总览</a> <span class="sep">/</span> <a href=${`#/task/${encodeURIComponent(a.task_id || "")}`}>任务 ${a.task_id || ""}</a> <span class="sep">/</span> <span>${a.id}</span></div>
    <div class="row"><h2>尝试 ${a.id}</h2>${badge(a.status)}<span class="spacer"></span>${copyButton(a.id)}</div>
    <dl class="kv">
      <dt>任务</dt><dd><a href=${`#/task/${encodeURIComponent(a.task_id || "")}`}><code class="mono">${a.task_id || "-"}</code></a> · ${data.task ? data.task.title : ""}（${data.task ? badge(data.task.status) : nothing}）</dd>
      <dt>代次 / 派单</dt><dd>g${a.generation} · dispatch ${badge(a.dispatch_state || "-")}${a.dispatch_msg_id ? html` <code class="mono">${a.dispatch_msg_id}</code>` : nothing}</dd>
      <dt>执行 session</dt><dd><code class="mono">${a.session_id || "-"}</code></dd>
      <dt>目录</dt><dd><code class="mono">${a.directory || "-"}</code></dd>
      <dt>创建 / 派单 / 启动</dt><dd>${ts(a.created_at)} / ${ts(a.dispatched_at)} / ${ts(a.started_at)}</dd>
      <dt>最近进展 / 观察</dt><dd>${ts(a.last_progress_at)} / ${ts(a.last_observed_at)}</dd>
      <dt>停止请求 / 已确认 / 现场释放</dt><dd>${ts(a.stop_requested_at)} / ${ts(a.stop_confirmed_at)} / ${ts(a.site_released_at)}</dd>
      <dt>结果收集</dt><dd>${ts(a.result_collected_at)}</dd>
      <dt>终态</dt><dd>${ts(a.terminal_at)}（${a.terminal_kind || "-"}）${a.terminal_reason ? html`<div class="note">${a.terminal_reason}</div>` : nothing}</dd>
    </dl>
    <h3>验收结论</h3>
    ${verdict}
    <h3>结果摘要</h3><pre class="log">${a.result_summary || "-"}</pre>
    ${a.result_path ? html`<div class="note">结果路径：<code class="mono">${a.result_path}</code></div>` : nothing}
    <h3>尝试操作${ro() ? "（read-only）" : ""}</h3>
    <div class="op-panel" id="attOps">${acts.length ? acts : html`<span class="muted">当前状态无可执行操作${ro() ? " / read-only" : ""}</span>`}</div>
    <div id="attemptFormHost">${renderVerifyForm("attempt:" + id)}</div>
    <h3>交付清单（${dels.length}）</h3>
    <div class="tablewrap"><table><thead><tr><th>交付物</th><th>类型</th><th>状态</th><th>检查时间</th><th>证据</th></tr></thead><tbody>
      ${dels.length ? repeat(dels, d => `${d.target || ""}:${d.kind || ""}`, d => html`<tr><td class="mono">${d.target || ""}</td><td>${d.kind || ""}</td><td>${badge(d.status || "-")}</td><td>${ts(d.checked_at)}</td><td class="mono">${d.evidence ? JSON.stringify(d.evidence) : "-"}</td></tr>`) : html`<tr><td colspan="5" class="muted">(none)</td></tr>`}
    </tbody></table></div>`;
}
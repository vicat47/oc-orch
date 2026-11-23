import {html, nothing} from "lit";
import {repeat} from "lit/directives/repeat.js";
import {state, ro} from "../state.js";
import {ts, badge, gateBadge, identity, sessionRunStatus, copyButton, unlockActionFor} from "../ui.js";

export function coordCard(c) {
  const ident = identity(c);
  const counts = c.counts || {};
  const href = `#/coordinator/${encodeURIComponent(c.session)}`;
  const unlock = unlockActionFor(c);
  const tips = (c.task_tips || []).slice(0, 3);
  const flagged = c.attention && c.attention_rank < 6;
  return html`<article class="card coord-card" data-key=${`coordinator:${c.session}`}>
    <div class="session-heading">${ident.badge}${sessionRunStatus(c, state.coordErr)}</div>
    <h3><a href=${href}>${ident.name}</a></h3>
    <div class="project-line">${(c.projects || []).join("、") || "未关联项目"}</div>
    ${c.missing_snapshot ? html`<div class="warn">本次未找到该会话，显示上次记录</div>` : nothing}
    <dl class="card-metrics">
      <div><dt>未完成</dt><dd>${counts.unfinished ?? "—"}</dd></div>
      <div><dt>待验收</dt><dd>${counts.awaiting_verification ?? "—"}</dd></div>
      <div><dt>受阻</dt><dd class=${counts.blocked ? "bad" : ""}>${counts.blocked ?? "—"}</dd></div>
    </dl>
    ${flagged ? html`<div class=${`attention-line ${c.attention_rank <= 1 ? "bad" : "warn"}`}>${c.attention}</div>` : nothing}
    <details class="card-details"><summary>会话详情</summary>
      <dl class="kv">
        <dt>会话 ID</dt><dd><code class="mono">${c.session}</code></dd>
        <dt>关联依据</dt><dd>${(c.sources || []).join("、") || "未知"}</dd>
        <dt>创建时间</dt><dd>${c.created_at_known ? ts(c.created_at) : "创建时间未知"}</dd>
        ${c.created_meta_stale || c.metadata_fresh === false ? html`<dt>会话信息</dt><dd class="warn">尚未更新 ${c.created_meta_error?.split(":")[0] || ""}</dd>` : nothing}
        <dt>最近进展</dt><dd>${ts(c.last_update)}</dd>
        <dt>项目租约</dt><dd>${(c.leases || []).length ? c.leases.map(l => html`<div>${l.project} · ${l.valid ? "有效" : "已到期"} ${l.recovery_blocked ? "恢复受阻" : l.failure_count ? `失败 ${l.failure_count} 次` : ""}</div>`) : "暂无"}</dd>
        <dt>调度限制</dt><dd>${(c.gates || []).length ? c.gates.map(gateBadge) : "暂无"}</dd>
        <dt>其他记录</dt><dd>已验收 ${counts.accepted ?? "—"} · 已取消 ${counts.cancelled ?? "—"} · 待处理通知 ${counts.notifications_pending ?? "—"}</dd>
      </dl>
      ${tips.map(t => html`<div class="task-line">${badge(t.status)}<a href=${`#/task/${encodeURIComponent(t.id)}`}>${t.title || t.id}</a></div>`)}
      ${counts.unfinished > tips.length || c.task_tips_truncated ? html`<div class="note">其余任务可在工作台查看。</div>` : nothing}
    </details>
    <div class="card-footer"><a class="button workspace-link" href=${href}>查看工作台 <span aria-hidden="true">→</span></a>
      ${unlock ? html`<button class="danger" data-act="unlock" data-sid=${c.session} data-project=${unlock.project} data-allow-provider=${unlock.allowProvider ? "1" : "0"} ?disabled=${ro()}>${unlock.label}</button>` : nothing}
      ${copyButton(c.session)}</div>
  </article>`;
}

function summaryLink(href, label, count) {
  return html`<a class="summary-link" href=${href}><span>${label}</span>
    <strong>${count ?? "—"}</strong><span class="summary-arrow" aria-hidden="true">↗</span></a>`;
}

export function renderOverview() {
  const meta = state.coordMeta;
  const departed = state.coordList.filter(sid => state.coordMap[sid].missing_snapshot).length;
  const outside = meta && state.ready && !state.errors.responsibilities && !state.coordErr
    ? state.resp.filter(r => !state.coordMap[r.owner]).length : null;
  const alerts = state.snap?.alerts?.filter(a => !a.task_id && !a.attempt_id).length;
  return html`<div class="page-heading"><h2>调度总览</h2></div>
    <div class="summary-band">
      ${summaryLink("#/tasks/unbound", "待分配任务", meta?.unbound_task_count)}
      ${summaryLink("#/diagnostics/responsibilities", "其他负责人待办", outside)}
      ${summaryLink("#/diagnostics/alerts", "未关联任务的告警", alerts)}
    </div>
    <div class="section-heading"><h3>调度会话 <span class="count">${meta ? state.coordList.length : "—"}</span></h3></div>
    <div class="grid-cards">${state.coordList.length
      ? repeat(state.coordList, sid => sid, sid => coordCard(state.coordMap[sid]))
      : html`<div class="empty">${meta ? "暂无关联的调度会话。" : "暂时无法读取调度会话。"}</div>`}</div>
    <details class="scope"><summary>数据范围与排序</summary>
      <p>待分配任务按全部任务统计。其他负责人待办统计当前读取的最多 200 条待办中，由列表以外的负责人处理的事项。告警统计本次读取的最多 50 条新告警。</p>
      <p>会话按创建时间从早到晚排列。仅收到通知的会话会标注身份。创建时间未知的排在最后，同组按会话 ID 排列。刷新时保留原有顺序。</p>
      ${departed ? html`<p>其中 ${departed} 个会话仅有上次记录。</p>` : nothing}
      ${meta?.metadata_fresh === false ? html`<p class="warn">部分会话缺少最新信息。</p>` : nothing}
    </details>`;
}

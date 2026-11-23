// 调度总览：全部已知调度者（稳定排序），下钻到工作台。
// 纯 Lit html 模板；交互经 data 属性冒泡由 app 统一委托，不查询 DOM、不绑定监听。
import {html, nothing} from "lit";
import {repeat} from "lit/directives/repeat.js";
import {state, ro} from "../state.js";
import {ts, badge, gateBadge, identity, copyButton, unlockActionFor} from "../ui.js";

export function coordCard(c) {
  const ident = identity(c), counts = c.counts || {};
  const staleTag = (c.created_meta_stale || c.metadata_fresh === false)
    ? html` <span class="badge b-blocked">元数据非最新${c.created_meta_error ? `（${c.created_meta_error.split(":")[0]}）` : ""}</span>`
    : nothing;
  const created = c.created_at_known ? ts(c.created_at) : html`<span class="warn">创建时间未知</span>`;
  // 元数据过期/失败时不把缓存当最新（不误称在线）
  const online = c.online === true && c.metadata_fresh !== false
    ? html`<span class="ok">在线 active</span>`
    : c.online === false && c.metadata_fresh !== false
      ? html`<span class="muted">不在 active 快照</span>`
      : html`<span class="warn">在线状态未知（元数据过期/失败时不把缓存当最新）</span>`;
  const leases = (c.leases || []).length
    ? repeat(c.leases, l => l.project || "", l => html`<div class=${l.valid ? "ok" : "bad"}>${l.project || ""}：${l.valid ? "租约有效" : "租约已到期"}${l.recovery_blocked ? html` <span class="bad">恢复阻塞</span>` : l.failure_count ? ` 失败 ${l.failure_count}` : nothing}</div>`)
    : html`<span class="muted">无项目租约</span>`;
  const gates = (c.gates || []).length ? c.gates.map(gateBadge) : html`<span class="muted">无</span>`;
  const tips = repeat((c.task_tips || []).slice(0, 3), t => t.id, t => html`<div class="task-line">${badge(t.status)}<a class="title" href=${`#/task/${encodeURIComponent(t.id)}`}>${t.title || t.id}</a><span class="spacer"></span><a href=${`#/task/${encodeURIComponent(t.id)}`}>详情</a></div>`);
  const tipMore = c.task_tips_truncated ? html`<div class="note">另有未终态任务未全部列出，进入工作台查看全量（全量计数不截断）。</div>` : nothing;
  const attnCls = c.attention_rank <= 1 ? "rank-high" : c.attention_rank <= 3 ? "rank-mid" : "";
  const unlockAction = unlockActionFor(c);
  const unlockBtn = unlockAction
    ? html`<button class="danger" data-act="unlock" data-sid=${c.session} data-project=${unlockAction.project} data-allow-provider=${unlockAction.allowProvider ? "1" : "0"} ?disabled=${ro()}>${unlockAction.label}</button>`
    : nothing;
  return html`<article class="card" data-key=${`coordinator:${c.session}`}>
    <div class="row"><h3><a href=${`#/coordinator/${encodeURIComponent(c.session)}`}>${ident.name}</a></h3>${ident.badge}<span class="spacer"></span>${copyButton(c.session)}</div>
    <div class=${`attn ${attnCls}`}><span>${c.attention || "关注"}</span></div>
    <dl class="kv">
      <dt>Session</dt><dd><code class="mono">${c.session}</code>${staleTag}${c.missing_snapshot ? html`<div class="warn">不在本期发现范围</div>` : nothing}</dd>
      <dt>身份级别</dt><dd>${ident.badge}<div class="muted">来源：${(c.sources || []).join("、") || "-"}</div></dd>
      <dt>创建时间</dt><dd>${created}</dd>
      <dt>在线状态</dt><dd>${online}</dd>
      <dt>项目</dt><dd>${(c.projects || []).length ? (c.projects || []).join("、") : html`<span class="muted">-</span>`}</dd>
      <dt>租约</dt><dd>${leases}</dd>
      <dt>闸门</dt><dd>${gates}</dd>
      <dt>计数</dt><dd><span class="badge">未终态 ${counts.unfinished ?? 0}</span> <span class="badge b-result_pending">待验收 ${counts.awaiting_verification ?? 0}</span> <span class="badge b-blocked">受阻 ${counts.blocked ?? 0}</span> <span class="badge b-accepted">已验收 ${counts.accepted ?? 0}</span> <span class="badge b-cancelled">已取消 ${counts.cancelled ?? 0}</span> <span class="badge">通知 ${counts.notifications_pending ?? 0}</span></dd>
      <dt>最近进展</dt><dd>${c.last_update ? ts(c.last_update) : html`<span class="muted">-</span>`}</dd>
    </dl>
    <div>${tips}${tipMore}</div>
    <div class="op-panel">${unlockBtn}<a class="button primary" href=${`#/coordinator/${encodeURIComponent(c.session)}`} style="display:inline-block;padding:5px 12px;border:1px solid #2ea04388;border-radius:6px;color:#56d364">查看工作台</a></div>
  </article>`;
}

export function renderOverview() {
  const meta = state.coordMeta || {};
  const departed = state.coordList.filter(sid => (state.coordMap[sid] || {}).missing_snapshot).length;
  // 未绑定任务取 /api/coordinators 的全量台账计数（不受快照 200 项限制）
  const fullUnbound = meta.unbound_task_count != null ? meta.unbound_task_count : null;
  const isCoord = sid => state.coordMap[sid] !== undefined;
  const oper = state.resp.filter(r => !isCoord(r.owner));
  const unattributed = (state.snap?.alerts || []).filter(a => !a.task_id && !a.attempt_id);

  const unboundPart = fullUnbound !== null
    ? (fullUnbound > 0
      ? html`<span class="bad"><a href="#/tasks/unbound" class="warn">未绑定任务 ${fullUnbound}（全量，含超限）</a></span>`
      : html`<span>未绑定任务 0（全量）</span>`)
    : html`<span class="warn">未绑定任务 ？（全量数据不可用）</span>`;
  const operPart = oper.length ? html`<span class="warn">operator/外部责任 ${oper.length}</span>` : html`operator/外部责任 0`;
  const alertPart = unattributed.length ? html`<span class="warn">无归属告警 ${unattributed.length}</span>` : html`无归属告警 0`;

  return html`<h2>调度总览</h2>
    <div class="attn rank-mid">${unboundPart} · ${operPart} · ${alertPart}</div>
    <div class="muted">
      共 ${state.coordList.length} 个已知会话（本期快照不在发现范围的已停用/解绑对象 ${departed} 仍保留并标注）：任务显式绑定/项目租约确认的调度者 ∪ 通知目标关联会话（非全体 OpenCode session）。
      <br>通知目标仅构成关联证据：单独历史通知不证明当前调度角色、在线或责任；worker/kind=test 等目标也可能出现，卡片以“仅通知关联 · 调度者角色未确认”标注，不会误称确认调度者。
      <br>排序：可靠创建时间从旧到新；未知创建时间排后、以 Session ID 稳定兜底。${meta.created_at_known != null ? html`创建时间已知 ${meta.created_at_known}，未知 ${meta.created_at_unknown}。` : nothing}
      ${(meta.metadata_error || !meta.metadata_fresh) ? html`<span class="warn">元数据过期/不可用：创建时间与在线可能显示未知，但不影响稳定排序。</span>` : nothing}
      <br>${meta.task_counts_note || ""}</div>
    <div class="grid-cards">${state.coordList.length ? repeat(state.coordList, sid => sid, sid => coordCard(state.coordMap[sid])) : html`<div class="empty">未发现调度者。请在任务显式绑定或协调者租约出现后自动呈现。</div>`}</div>`;
}
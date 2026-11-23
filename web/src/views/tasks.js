import {html, nothing} from "lit";
import {repeat} from "lit/directives/repeat.js";
import {state} from "../state.js";
import {ts, badge} from "../ui.js";

function taskTable(list, {unbound = false, empty = "暂无任务"} = {}) {
  return html`<div class="tablewrap task-table"><table><thead><tr>
    <th>任务</th><th>状态</th>${unbound ? nothing : html`<th>指定调度者</th>`}<th>${unbound ? "最近更新" : "最近进展"}</th>
    </tr></thead><tbody>${list.length ? repeat(list, t => t.id, t => html`<tr>
      <td><a class="task-title" href=${`#/task/${encodeURIComponent(t.id)}`}>${t.title || t.id}</a>
        <div class="note">${t.project} <code>${t.id}</code></div></td>
      <td>${badge(t.status)}${t.blocked_reason ? html`<div class="note">${t.blocked_reason}</div>` : nothing}</td>
      ${unbound ? nothing : html`<td>${t.coordinator_session ? html`<a href=${`#/coordinator/${encodeURIComponent(t.coordinator_session)}`}>${state.coordMap[t.coordinator_session]?.title || t.coordinator_session}</a>` : "未单独指定"}</td>`}
      <td>${ts(unbound ? t.updated_at : t.last_progress_at)}</td>
    </tr>`) : html`<tr><td colspan=${unbound ? 3 : 4}>${empty}</td></tr>`}</tbody></table></div>`;
}

export function renderTasksView() {
  const tasks = state.snap?.tasks || [];
  const total = state.snap?.counts?.tasks;
  const q = (state.search || "").toLowerCase().trim();
  const list = q ? tasks.filter(t => [t.id, t.title, t.project].some(v => v?.toLowerCase().includes(q))) : tasks;
  return html`<h2>全局任务</h2>
    <div class="row"><label class="search-field"><input type="text" id="taskSearch" value=${state.search}
      aria-label="搜索已加载的任务" placeholder="搜索任务标题、项目或 ID"></label>
      <span class="note">${q ? `找到 ${list.length} 项 · ` : ""}已加载 ${tasks.length} 项${total != null ? `，共 ${total} 项` : ""}</span></div>
    ${taskTable(list, {empty: q ? "没有匹配的任务" : "暂无任务"})}`;
}

export function renderUnboundTasks() {
  const rows = state.unboundRows || [];
  return html`<div class="breadcrumb"><a href="#/">总览</a> / 待分配任务</div>
    <h2>待分配任务</h2><p class="note">这些任务尚无调度者负责。已加载 ${rows.length} 项，共 ${state.unboundFull} 项。</p>
    ${taskTable(rows, {unbound: true, empty: "暂无待分配任务"})}
    <div class="op-panel">${state.unboundTruncated ? html`<button class="primary" data-act="unboundmore" ?disabled=${state.unboundLock}>继续加载</button>` : nothing}<a href="#/tasks">查看全局任务</a></div>`;
}

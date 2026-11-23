// 全局任务检索：当前快照 ≤200 过滤检索；未绑定任务走全量 keyset 分页（state.unbound*）。纯 Lit html 模板。
// 搜索输入由 app 统一处理（id=taskSearch）；“继续加载”经 data-act=unboundmore 冒泡。
import {html, nothing} from "lit";
import {repeat} from "lit/directives/repeat.js";
import {state} from "../state.js";
import {ts, badge} from "../ui.js";

export function renderTasksView() {
  const snap = state.snap || {};
  const q = (state.search || "").toLowerCase().trim();
  let list = snap.tasks || [];
  // total 范围说明：不把快照子集声称全量
  const total = snap.counts && snap.counts.tasks != null
    ? html`全库 ${snap.counts.tasks} 项；当前快照载入 ${list.length} 项（≤200，超限任务不计入本列表）`
    : html`当前快照 ${list.length} 项`;
  if (q) {
    list = list.filter(t => (t.id || "").toLowerCase().includes(q)
      || (t.title || "").toLowerCase().includes(q)
      || (t.project || "").toLowerCase().includes(q));
  }
  return html`<h2>全局任务检索</h2>
    <div class="row" style="margin-bottom:10px"><label class="inline" style="flex:1;max-width:480px">按 ID / 标题 / 项目过滤
      <input type="text" id="taskSearch" value=${state.search} placeholder="输入关键字过滤当前快照"></label><span class="muted">${total}</span></div>
    <div class="tablewrap"><table><thead><tr><th>Task</th><th>标题</th><th>状态</th><th>协调者</th><th>进展</th></tr></thead><tbody>
      ${list.length ? repeat(list, t => t.id, t => html`<tr><td><a href=${`#/task/${encodeURIComponent(t.id)}`}><code class="mono">${t.id}</code></a><div class="muted">${t.project || ""}</div></td><td><a href=${`#/task/${encodeURIComponent(t.id)}`}>${t.title || ""}</a></td><td>${badge(t.status)}${t.blocked_reason ? html`<div class="muted">${t.blocked_reason}</div>` : nothing}</td><td>${t.coordinator_session ? html`<a href=${`#/coordinator/${encodeURIComponent(t.coordinator_session)}`}><code class="mono">${t.coordinator_session}</code></a>` : html`<span class="warn">未绑定</span>`}</td><td>${ts(t.last_progress_at)}</td></tr>`) : html`<tr><td colspan="5" class="muted">${q ? "无匹配" : "(none)"}</td></tr>`}
    </tbody></table></div>`;
}

export function renderUnboundTasks() {
  const rows = state.unboundRows || [];
  return html`<h2>未绑定任务（全量 ${state.unboundFull} 项）</h2>
    <div class="note">未绑定 = 无显式 coordinator_session 且其 project 无项目租约持有者；全量 keyset 分页（按任务 ID 升序），不受快照 200 项限制，不无界一次性输出。</div>
    <div class="tablewrap"><table><thead><tr><th>Task</th><th>标题</th><th>状态</th><th>受阻</th><th>最近更新</th></tr></thead><tbody>
      ${rows.length ? repeat(rows, t => t.id, t => html`<tr><td><a href=${`#/task/${encodeURIComponent(t.id)}`}><code class="mono">${t.id}</code></a><div class="muted">${t.project || ""}</div></td><td><a href=${`#/task/${encodeURIComponent(t.id)}`}>${t.title || ""}</a></td><td>${badge(t.status)}</td><td>${t.blocked_reason || "-"}</td><td>${ts(t.updated_at)}</td></tr>`) : html`<tr><td colspan="5" class="muted">(none)</td></tr>`}
    </tbody></table></div>
    <div class="op-panel"><span class="note">已加载 ${rows.length} / ${state.unboundFull} 项 · 按任务 ID 升序</span><span class="spacer"></span>${state.unboundTruncated ? html`<button class="primary" data-act="unboundmore" ?disabled=${state.unboundLock}>继续加载</button>` : nothing}<a href="#/tasks">返回任务检索</a></div>`;
}
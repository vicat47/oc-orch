// 调度者工作台：任务 / 待办 / 活动，默认任务；全量 keyset 分页由 state.coordPages 累积。
// 纯 Lit html 模板；交互经 data 属性冒泡（data-tab / data-act=acknote|unlock|coordmore）由 app 统一委托。
import {html, nothing} from "lit";
import {repeat} from "lit/directives/repeat.js";
import {state, ro} from "../state.js";
import {ts, badge, identity, sessionRunStatus, gateBadge, copyButton, unlockActionFor} from "../ui.js";

function tasksTab(sid, pag) {
  const tasks = pag.tasks || [];
  const unfinished = tasks.filter(t => t.status !== "accepted" && t.status !== "cancelled");
  const accepted = tasks.filter(t => t.status === "accepted");
  const cancelled = tasks.filter(t => t.status === "cancelled");
  const trow = t => html`<div class="task-line coord-task">
    <div class="row">${badge(t.status)}<a class="title" href=${`#/task/${encodeURIComponent(t.id)}`}>${t.title || t.id}</a></div>
    <div class="task-context"><span>${t.project || "项目未知"} · ${t.owned_via === "explicit" ? "任务单独指定" : "由项目调度者负责"}</span>
      ${t.current_attempt ? html`<a href=${`#/attempt/${encodeURIComponent(t.current_attempt)}`}>查看本次执行</a>` : nothing}</div>
    ${t.blocked_reason ? html`<div class="warn">受阻原因 · ${t.blocked_reason}</div>` : nothing}
  </div>`;
  // 分页按任务 ID 升序，不是按 updated 时间；不声称“最近”。
  const histRow = label => t => html`<div class="task-line"><span>${badge(t.status)}</span><a href=${`#/task/${encodeURIComponent(t.id)}`}>${t.title || t.id}</a><span class="spacer"></span><span class="note">${label} ${ts(t.updated_at)}</span></div>`;
  const acc = repeat(accepted.slice(0, 20), t => t.id, histRow("已验收"));
  const can = repeat(cancelled.slice(0, 20), t => t.id, histRow("已取消"));
  return html`<h3>未完成任务 <span class="count">${unfinished.length}</span></h3><div class="note">已加载 ${tasks.length} 项，共 ${pag.full} 项</div>
    ${unfinished.length ? repeat(unfinished, t => t.id, trow) : html`<div class="empty">当前列表中没有未完成任务。</div>`}
    ${accepted.length ? html`<h3>已验收 · ${accepted.length}</h3>${acc}${accepted.length > 20 ? html`<div class="note">… 已加载其中前 20 项，其余请使用任务检索</div>` : nothing}` : nothing}
    ${cancelled.length ? html`<h3>已取消 · ${cancelled.length}</h3>${can}${cancelled.length > 20 ? html`<div class="note">… 已加载其中前 20 项，其余请使用任务检索</div>` : nothing}` : nothing}
    ${pag.truncated ? html`<div class="op-panel"><button class="primary" data-act="coordmore" data-sid=${sid} ?disabled=${pag.lock}>继续加载</button></div>` : nothing}
`;
}

function todosTab(notes, resp) {
  const rrow = r => html`<div class="task-line"><span>${badge(r.state)}</span><span class="mono">${r.id || "-"}</span><div><div>${r.owner || "-"} → ${r.target_session || "-"}</div><div class="note">等待条件：${r.wait_condition || "-"} · 下一步 ${r.next_action || "-"} · 期限 ${ts(r.deadline)}</div></div></div>`;
  const actionable = notes.filter(n => ["pending", "inflight", "sent"].includes(n.state));
  return html`<h3>待办事项</h3>
    ${resp.length ? repeat(resp, r => r.id || "", rrow) : html`<div class="empty">暂无待办事项。</div>`}
    ${actionable.length ? html`<h3>待处理通知</h3>${repeat(actionable, n => n.id, n => html`<div class="task-line"><span>${badge(n.state)}</span><span class="mono">${n.id}</span><span>${n.kind} · ${n.summary || ""}</span><span class="spacer"></span>${ro() ? nothing : html`<button class="primary" data-act="acknote" data-id=${n.id}>确认收到</button>`}</div>`)}` : nothing}`;
}

function activityTab(sid, notes) {
  const nrow = n => html`<div class="task-line"><span>${badge(n.state)}</span><span class="mono">${n.id}</span><span>${n.kind}</span><div class="note">摘要：${n.summary || "-"} · 发送于 ${ts(n.sent_at)}</div><span class="spacer"></span></div>`;
  const inputs = (state.inputs || []).filter(i => (i.session_id || i.session) === sid);
  return html`<h3>通知记录</h3>
    ${notes.length ? repeat(notes, n => n.id, nrow) : html`<div class="empty">无通知记录。</div>`}
    <h3>发给此会话的指令</h3>
    ${inputs.length ? repeat(inputs, i => i.input_id || i.message_id || "", i => html`<div class="task-line"><span class="mono">${i.input_id || "-"}</span><span>${i.intent || "-"} · 受理 ${i.admission || "-"} → 投递 ${i.delivery || "-"} → 业务ACK ${i.business_ack || "-"}</span><span class="note">下一步 ${i.next_action || "-"}</span></div>`) : html`<div class="empty">无输入回执。</div>`}`;
}

// R2 业务健康：进程健康≠业务进展/完成；控制维度独立展示；只在“唯一且合法项目”
// 显示暂停/继续按钮（无 project / 多项目不猜；read_only 隐藏写按钮）。
function healthTab(sid, data) {
  const h = state.healthBySid[sid];
  const body = h?.body;
  const tasks = (body?.tasks) || [];
  const projects = (data.coordinator?.projects || []);
  const single = projects.length === 1 ? projects[0] : null;
  const r = ro();
  const controlTask = tasks.find(t => t.project === single && t.control);
  const intent = controlTask?.control?.project_intent;
  const rev = controlTask?.control?.project_revision;
  const epoch = controlTask?.control?.epoch;
  const hasControlVersion = typeof rev === "number" && typeof epoch === "number";
  const showResume = !r && single && intent === "paused" && hasControlVersion;
  const showPause = !r && single && intent !== "paused" && hasControlVersion;

  const controlBits = t => {
    const c = t.control || {};
    const bits = [
      c.project_intent ? `意图 ${c.project_intent}` : null,
      c.turn_held ? "hold" : null,
      c.user_turn_active ? "回合中" : null,
      c.provider_gate ? `provider ${c.provider_gate}` : null,
    ].filter(Boolean).join(" · ");
    const ver = c.project_revision != null ? `rev ${c.project_revision}` : "";
    const ep = c.epoch != null ? `epoch ${c.epoch}` : "";
    const blocking = (c.blocking || []).join("、");
    return html`${bits || html`<span class="muted">无控制记录</span>`}${ver || ep ? html`<div class="muted">${ver}${ver && ep ? " · " : ""}${ep}</div>` : nothing}${blocking ? html`<div class="bad">${blocking}</div>` : nothing}`;
  };
  const trow = t => {
    const resp = (t.responsibilities || [])[0] || {};
    return html`<tr>
      <td><a href=${`#/task/${encodeURIComponent(t.task_id || "")}`}><code class="mono">${t.task_id || "-"}</code></a><div class="muted">${t.project || t.coordinator_session || ""}</div></td>
      <td>${badge(t.state)}${t.process_healthy != null ? html`<div class="muted">proc ${t.process_healthy ? "ok" : "down"}</div>` : nothing}</td>
      <td class="mono">${t.owner || "-"}</td>
      <td>${t.next_action || "-"}</td>
      <td>${resp.deadline != null ? ts(resp.deadline) : "-"}<div class="muted">下一步 ${resp.next_check_at != null ? ts(resp.next_check_at) : "-"}</div></td>
      <td>${resp.budget_used != null ? `${resp.budget_used}/${resp.max_budget ?? "-"}` : "-"}</td>
      <td>${t.evidence_coverage === true ? html`<span class="ok">完整</span>` : t.evidence_coverage === false ? html`<span class="warn">不足</span>` : html`<span class="muted">-</span>`}</td>
      <td>${ts(t.latest_business_event_at)}</td>
      <td>${controlBits(t)}</td>
    </tr>`;
  };

  const proc = body?.process || {};
  const procLine = proc.healthy == null
    ? html`<span class="muted">进程状态未知</span>`
    : proc.healthy
      ? html`<span class="ok">进程正常</span> ${proc.connected != null ? `connected=${proc.connected}` : ""}${proc.heartbeat_age_s != null ? html` · 心跳 ${proc.heartbeat_age_s}s 前` : ""}${proc.last_cycle_at != null ? html` · 上轮 ${ts(proc.last_cycle_at)}` : ""}`
      : html`<span class="bad">进程异常</span>`;

  const runtime = body?.runtime || {};
  const runtimeBits = [];
  if (runtime.release_id) runtimeBits.push(html`release ${runtime.release_id}`);
  if (runtime.manifest_verified === true) runtimeBits.push(html`<span class="ok">manifest 校验通过</span>`);
  else if (runtime.source) runtimeBits.push(html`<span class="muted">${runtime.source}</span>`);
  if (runtime.package_version) runtimeBits.push(html`包版本 ${runtime.package_version}`);
  if (runtime.email_default_enabled === false) runtimeBits.push(html`邮件默认关闭`);
  if (runtime.native_trusted_chat_enabled === false) runtimeBits.push(html`native 对话未启用`);

  const scopeNote = body?.coverage
    ? (body.coverage.complete ? "覆盖完整" : `部分覆盖${body.coverage.next_after_id ? "，可续页" : ""}`)
    : "覆盖未知";

  const ctrl = !single
    ? html`<div class="note">${projects.length ? "此会话负责多个项目，请按项目选择暂停或恢复。" : "此会话尚未关联项目。"}</div>`
    : html`<div class="row" style="margin:6px 0">
        <span class="muted">项目 <code class="mono">${single}</code>：${intent ?? "未知"}</span>${hasControlVersion ? html`<span class="muted"> · rev ${rev} · epoch ${epoch}</span>` : nothing}<span class="spacer"></span>
        ${showResume ? html`<button class="primary" data-act="projresume" data-sid=${sid} data-project=${single} ?disabled=${r}>继续</button>` : nothing}
        ${showPause ? html`<button class="danger" data-act="projpause" data-sid=${sid} data-project=${single} ?disabled=${r}>暂停</button>` : nothing}
        ${!hasControlVersion && single ? html`<span class="muted">缺少项目版本，暂时无法操作</span>` : nothing}
      </div>`;

  return html`<div id="bizHealth">
    <h3>业务健康</h3>
    <div class="note">任务完成以验收结论为准。 ${scopeNote}${h?.error ? html` <span class="bad">刷新失败，显示上次记录 · ${h.error.message}</span>` : nothing}</div>
    <div class="note">${procLine}</div>
    ${ctrl}
    <div class="tablewrap"><table><thead><tr><th>任务</th><th>状态</th><th>负责人</th><th>下一动作</th><th>期限 / 下次检查</th><th>预算</th><th>证据</th><th>最近业务事件</th><th>调度限制</th></tr></thead><tbody>
      ${tasks.length ? repeat(tasks, t => t.task_id || JSON.stringify(t), trow)
        : html`<tr><td colspan="9" class="muted">${h ? "该会话当前无未完成业务健康记录。" : "正在等待健康数据。"}</td></tr>`}
    </tbody></table></div>
    ${runtimeBits.length ? html`<div class="note" id="runtimeIdentity">运行身份：${runtimeBits}</div>` : nothing}
  </div>`;
}

export function renderCoordinator(sid, data = {}) {
  const c = data.coordinator || {};
  const key = `coordinator:${sid}`;
  const stale = state.details.get(key)?.error || state.pendingRoute.has(key);
  const ident = identity(c);
  // 分页累积来源：state.coordPages（data.js 合并本页），缺失时回退到本次传入数据
  const pag = state.coordPages[sid] || {
    tasks: data.tasks || [],
    full: data.tasks_full_count ?? (data.tasks || []).length,
    after: data.next_after_id || null,
    truncated: !!data.tasks_truncated,
    lock: false,
  };
  const tab = state.tab.coordinator || "tasks";
  const created = c.created_at_known ? ts(c.created_at) : html`<span class="warn">创建时间未知</span>`;
  // 元数据过期/失败时不把缓存当最新（与总览一致）
  const leases = (c.leases || []).length
    ? repeat(c.leases, l => l.project || "", l => html`<div class=${l.valid ? "ok" : "bad"}>${l.project || ""}：${l.valid ? "租约有效" : "已到期"}${l.recovery_blocked ? html` <span class="bad">恢复阻塞</span>` : l.failure_count ? html` <span class="warn">失败 ${l.failure_count}</span>` : nothing}<span class="muted"> 到期 ${ts(l.expires_at)}</span></div>`)
    : html`<span class="muted">无项目租约</span>`;
  const gates = (c.gates || []).length
    ? repeat(c.gates, g => `${g.state}:${g.source || ""}`, g => html`<div class=${g.state === "manual-blocked" ? "bad" : "warn"}>${gateBadge(g)}${g.reason ? html`<div class="muted">${g.reason}</div>` : nothing}</div>`)
    : html`<span class="muted">暂无调度限制</span>`;
  const unlockAction = unlockActionFor(c);
  const unlock = unlockAction
    ? html`<button class="danger" data-act="unlock" data-sid=${sid} data-project=${unlockAction.project} data-allow-provider=${unlockAction.allowProvider ? "1" : "0"} ?disabled=${ro()}>${unlockAction.label}</button>`
    : nothing;
  const counts = c.counts || {};
  const countsRow = Object.keys(counts).length
    ? html`未完成 ${counts.unfinished ?? 0} · 待验收 ${counts.awaiting_verification ?? 0} · 受阻 ${counts.blocked ?? 0} · 已验收 ${counts.accepted ?? 0} · 已取消 ${counts.cancelled ?? 0} · 通知 ${counts.notifications_pending ?? 0}`
    : "-";
  // 回合 hold 的只读展示与两个明确入口：登记已完成用户回合（默认）/
  // 运维强制解除（兜底）。两者都只走共享控制用例；不撤销项目暂停/provider。
  const sc = data.session_control || {};
  const hold = sc.hold;
  const barrier = sc.barrier;
  const singleProject = (c.projects || []).length === 1 ? c.projects[0] : null;
  // 候选绑定检查时的控制 revision；hold/中断变化后旧候选不再展示，
  // 避免对新建的 hold 使用过期消息身份。
  const candidate = state.turnCandidates[sid]
    && state.turnCandidates[sid].revision === sc.revision
    ? state.turnCandidates[sid] : null;
  const holdRow = sc.ok === false
    ? html`<span class="bad">回合控制不可读 · ${sc.error || "未知错误"}</span>`
    : hold
      ? html`<div class="bad">自动唤醒已暂停 · ${hold.reason || "unknown"}<span class="muted"> ${ts(hold.held_at)}</span></div>${barrier ? html`<div class="warn">正在等待用户回合结束 · <code class="mono">${barrier.message_id || "-"}</code> · ${ts(barrier.origin_at)}</div>` : html`<div class="muted">暂无待结束的用户回合</div>`}`
      : html`<span class="muted">自动唤醒未暂停</span>`;
  const candidateRow = candidate
    ? candidate.eligible
      ? html`<div class="ok">可登记的用户输入：<code class="mono">${candidate.message_id}</code> · ${ts(candidate.created)} · ${candidate.outcome}</div>`
      : html`<div class="warn">不可登记：${candidate.reason || "unknown"}${candidate.message_id ? html` · 最新用户输入 <code class="mono">${candidate.message_id}</code>` : nothing}</div>`
    : nothing;
  const holdActions = hold && !ro() && singleProject && !barrier && sc.ok !== false
    ? html`<div class="op-panel">
        <button data-act="checkturn" data-sid=${sid} data-project=${singleProject}>检查可登记的用户回合</button>
        ${candidate?.eligible ? html`<button class="primary" data-act="registerturn" data-sid=${sid} data-project=${singleProject}>登记已完成的回合</button>` : nothing}
        <button class="danger" data-act="releasehold" data-sid=${sid} data-project=${singleProject}>强制恢复自动唤醒</button>
        <span class="note">登记需要已完成的用户输入。强制恢复后，系统仍会检查项目暂停和其他限制。</span>
      </div>
      ${candidateRow}`
    : nothing;
  const body = tab === "tasks" ? tasksTab(sid, pag)
    : tab === "todos" ? todosTab(data.notifications || [], data.responsibilities || [])
    : tab === "health" ? healthTab(sid, data)
    : activityTab(sid, data.notifications || []);
  return html`<div class="breadcrumb"><a href="#/">总览</a> <span class="sep">/</span> <span>${ident.name}</span></div>
    <div class="row workspace-heading"><h2>${ident.name}</h2>${sessionRunStatus(c, stale)}${ident.badge}<span class="spacer"></span>${copyButton(sid)}${unlock}</div>
    ${(c.projects || []).length === 1 && !ro() ? html`<div class="op-panel">
      <button data-act="usersend" data-sid=${sid} data-project=${c.projects[0]}>补充指令</button>
      <button data-act="userobserve" data-sid=${sid} data-project=${c.projects[0]}>检查上一条指令</button>
</div>` : nothing}
    <details class="scope"><summary>会话与项目详情</summary><dl class="kv">
      <dt>会话 ID</dt><dd><code class="mono">${sid}</code></dd>
      <dt>创建时间</dt><dd>${created}</dd>
      <dt>项目</dt><dd>${(c.projects || []).length ? (c.projects || []).join("、") : html`<span class="muted">-</span>`}</dd>
      <dt>租约</dt><dd>${leases}</dd>
      <dt>调度限制</dt><dd>${gates}</dd>
      <dt>全部任务与通知</dt><dd>${countsRow}</dd>
    </dl></details>
    ${hold || sc.ok === false ? html`<div class="attn rank-mid"><strong>回合控制</strong>${holdRow}</div>` : nothing}
    ${holdActions}
    <div class="tabs" role="tablist" aria-label="工作台分区">
      <button role="tab" aria-selected=${tab === "tasks" ? "true" : "false"} data-tab="tasks">任务</button>
      <button role="tab" aria-selected=${tab === "todos" ? "true" : "false"} data-tab="todos">待办</button>
      <button role="tab" aria-selected=${tab === "health" ? "true" : "false"} data-tab="health">健康</button>
      <button role="tab" aria-selected=${tab === "activity" ? "true" : "false"} data-tab="activity">活动</button>
    </div>
    ${body}`;
}

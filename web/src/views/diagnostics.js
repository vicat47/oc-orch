// 系统诊断：通知/告警表格、协调者 manual 解锁、Supervisor、租约退避、发送受管输入、
// 受管输入/白名单观察、后续责任、最近审计。纯 Lit html 模板。
// 交互经 data 属性冒泡由 app 统一委托（ackchecked/ackalerts/unlock/supstart/supstop/data-managed）。
import {html, nothing} from "lit";
import {repeat} from "lit/directives/repeat.js";
import {state, ro} from "../state.js";
import {ts, badge, gateBadge, fmtAge, unlockActionFor, managedBadge, watchEnabledBadge, watchObservation} from "../ui.js";

const managedSession = i => i.session_id || i.session || "-";

function notifSection(ns) {
  return html`<section id="diag-notif" aria-label="通知与告警">
    <h2>通知</h2>
    <div class="row" style="margin-bottom:8px">
      <span class="muted" id="notifMeta">已列出 ${ns.length} 条通知</span>
      <span class="spacer"></span>
      <button id="ackCheckedBtn" data-act="ackchecked" class="primary" ?disabled=${ro()}>确认收到所选通知</button>
    </div>
    <div class="tablewrap"><table id="notifTable"><thead><tr><th>编号 / 类型</th><th>状态</th><th>目标会话</th><th>摘要</th><th>最近错误</th></tr></thead><tbody>
      ${ns.length ? repeat(ns, n => n.id, n => html`<tr><td><label class="inline"><input type="checkbox" class="noteck" data-id=${n.id} checked> <code class="mono">${n.id}</code></label><div class="muted">${n.kind}</div></td><td>${badge(n.state)}</td><td><code class="mono">${n.target_session || "-"}</code>${n.task_id || n.attempt_id ? html`<div class="muted">${n.task_id || ""} ${n.attempt_id || ""}</div>` : nothing}</td><td>${n.summary || ""}${n.missing_deliverables && n.missing_deliverables.length ? html`<div class="bad">missing ${n.missing_deliverables.join("、")}</div>` : nothing}</td><td class="mono">${n.last_error || "-"}</td></tr>`) : html`<tr><td colspan="5" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>
    <div class="note">确认收到后，任务仍需单独验收。</div>
  </section>`;
}

function alertSection(alerts, title = "告警") {
  return html`<section id="diag-alerts" aria-label="告警">
    <h2>${title}</h2>
    <div class="row" style="margin-bottom:8px">
      <span class="muted" id="alertMeta">当前快照中有 ${alerts.length} 条，上限 50 条</span>
      <span class="spacer"></span>
      <button id="ackAlertsBtn" data-act="ackalerts" class="primary" ?disabled=${ro()}>确认所选告警</button>
    </div>
    <div class="tablewrap"><table id="alertTable"><thead><tr><th>ID</th><th>级别</th><th>标题</th><th>归属</th><th>详情</th></tr></thead><tbody>
      ${alerts.length ? repeat(alerts, a => a.id, a => html`<tr><td><label class="inline"><input type="checkbox" class="alck" data-id=${a.id} checked> <code class="mono">${a.id}</code></label></td><td>${a.severity || ""}</td><td>${badge(a.kind)} ${a.title || ""}</td><td>${a.task_id || a.attempt_id ? html`<span class="muted">task ${a.task_id || ""} attempt ${a.attempt_id || ""}</span>` : html`<span class="warn">未关联任务</span>`}</td><td><details><summary class="muted">查看原文</summary><pre>${a.detail || ""}</pre></details></td></tr>`) : html`<tr><td colspan="5" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>

  </section>`;
}

function gatesSection() {
  const coords = state.coordList.map(sid => state.coordMap[sid]).filter(Boolean);
  return html`<section id="diag-gates" aria-label="协调者解锁">
    <h2>恢复调度</h2>
    <div id="gatesRoot"><div class="tablewrap"><table><thead><tr><th>会话</th><th>调度限制</th><th></th></tr></thead><tbody>
      ${coords.length ? repeat(coords, c => c.session, c => {
        const ua = unlockActionFor(c);
        return html`<tr><td><a href=${`#/coordinator/${encodeURIComponent(c.session)}`}><code class="mono">${c.session}</code></a></td><td>${(c.gates || []).length ? c.gates.map(gateBadge) : html`<span class="muted">无闸门</span>`}</td><td>${ua ? html`<button class="danger" data-act="unlock" data-sid=${c.session} data-project=${ua.project} data-allow-provider=${ua.allowProvider ? "1" : "0"} ?disabled=${ro()}>${ua.label}</button>` : html`<span class="muted">无可执行操作</span>`}</td></tr>`;
      }) : html`<tr><td colspan="3" class="muted">暂无记录</td></tr>`}
    </tbody></table></div></div>
    <div class="note">解除限制需要填写原因并输入 UNLOCK。遇到服务商限制时，恢复后会先重新探测。</div>
  </section>`;
}

function supSection() {
  return html`<section id="diag-supervisor" aria-label="后台调度服务">
    <h2>后台调度服务</h2>
    <div class="op-panel" id="supRoot"><button class="primary" data-act="supstart" ?disabled=${ro()}>启动调度服务</button><button class="danger" data-act="supstop" ?disabled=${ro()}>停止调度服务</button></div>
    <div class="note">启动需输入 START，停止需输入 STOP。</div>
  </section>`;
}

function leasesSection(snap) {
  const leases = snap.leases || [];
  const back = snap.provider_backoffs || {};
  const gates = snap.target_gates || [];
  return html`<section id="diag-leases" aria-label="租约与退避">
    <h2>项目租约与服务商限制</h2>
    <details id="meta"><summary>查看原始记录</summary>
      <h3>Leases</h3><pre>${JSON.stringify(leases, null, 1)}</pre>
      <h3>Provider backoff</h3><pre>${JSON.stringify(back, null, 1)}</pre>
      <h3>Target gates</h3><pre>${JSON.stringify(gates, null, 1)}</pre>
    </details>
  </section>`;
}

function sendSection() {
  const r = ro();
  return html`<section id="diag-sup-input" aria-label="发送受管输入">
    <h2>发送会话指令</h2>
    <div id="sendFormHost"><form id="sendForm" data-form="send" class="row">
      <label>会话 ID<input type="text" name="session" placeholder="ses_…" required ?disabled=${r}></label>
      <label>处理方式<select name="intent" ?disabled=${r}><option value="execute">执行</option><option value="park">暂存</option><option value="record">仅记录</option></select></label>
      <label>文本<input type="text" name="text" required ?disabled=${r}></label>
      <label>负责人<input type="text" name="owner" required ?disabled=${r}></label>
      <label>请求 ID<input type="text" name="request_id" required ?disabled=${r}></label>
      <label>项目<input type="text" name="project" ?disabled=${r}></label>
      <label>任务 ID<input type="text" name="task" ?disabled=${r}></label>
      <label>尝试 ID<input type="text" name="attempt" ?disabled=${r}></label>
      <label>目标会话<input type="text" name="target_session" ?disabled=${r}></label>
      <button class="primary" type="submit" ?disabled=${r}>发送</button>
    </form></div>
    <div class="note">可在指令记录中查看投递和执行进度。</div>
  </section>`;
}

function managedSection(inputs, watches) {
  const r = ro();
  return html`<section id="diag-managed" aria-label="受管输入与白名单">
    <h2>指令记录与会话观察</h2>
    <div class="muted" id="managedMeta">已读取 ${inputs.length} 条指令 · ${watches.length} 个观察会话</div>
    <div class="tablewrap"><table><thead><tr><th>指令 / 消息</th><th>会话</th><th>负责人</th><th>处理方式</th><th>受理</th><th>投递</th><th>执行</th><th>处理确认</th><th>年龄 / 下一动作</th><th>操作</th></tr></thead><tbody id="managedBody">
      ${inputs.length ? repeat(inputs, i => i.input_id || i.message_id || JSON.stringify(i), i => html`<tr>
        <td><code class="mono">${i.input_id || "-"}</code><div class="muted">${i.message_id || "-"}</div></td>
        <td><code class="mono">${managedSession(i)}</code></td>
        <td>${i.owner || "-"}</td>
        <td>${i.intent || "-"}</td>
        <td>${managedBadge(i.admission)}</td>
        <td>${managedBadge(i.delivery)}</td>
        <td>${managedBadge(i.execution)}</td>
        <td>${managedBadge(i.business_ack)}</td>
        <td>${fmtAge(i.pending_age_ms)}<div class="muted">next ${i.next_action || "-"}</div></td>
        <td>${r ? "-" : html`${["flush", "cancel"].map(a => html`<button data-managed=${a} data-id=${i.input_id} data-rev=${i.revision == null ? "" : i.revision}>${a === "flush" ? "立即投递" : "取消"}</button>`)}${i.business_ack !== "acknowledged" ? html`<button class="primary" data-managed="ack" data-id=${i.input_id} data-rev=${i.revision == null ? "" : i.revision}>确认处理</button>` : nothing}`}</td>
      </tr>`) : html`<tr><td colspan="10" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>
    <div class="muted" style="margin-top:8px">已登记的观察会话</div>
    <div class="tablewrap"><table><thead><tr><th>会话</th><th>负责人</th><th>项目</th><th>状态</th><th>最长等待时间</th><th>版本 / 待处理</th><th>最近观察</th><th>下一动作</th></tr></thead><tbody id="watchBody">
      ${watches.length ? repeat(watches, w => w.session_id || w.session || JSON.stringify(w), w => html`<tr>
        <td><code class="mono">${w.session_id || w.session || "-"}</code></td>
        <td>${w.owner || "-"}</td>
        <td>${w.project || "-"}</td>
        <td>${watchEnabledBadge(w.enabled)}</td>
        <td>${w.max_pending_age_ms == null ? "-" : w.max_pending_age_ms + " ms"}</td>
        <td>${w.revision == null ? "-" : w.revision}${w.pending_count != null ? html`<div class="muted">pending ${w.pending_count}</div>` : nothing}</td>
        <td>${watchObservation(w)}</td>
        <td>${w.next_action || "-"}</td>
      </tr>`) : html`<tr><td colspan="8" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>
    <div class="note">操作前需输入对应的指令 ID。</div>
  </section>`;
}

function respSection(resp, title = "待跟进事项") {
  return html`<section>
    <h2>${title}</h2>
    <div class="muted" id="respMeta">当前列出 ${resp.length} 条，从最多 200 条未关闭事项中读取。</div>
    <div class="tablewrap"><table><thead><tr><th>ID / 状态</th><th>任务 / 尝试</th><th>负责人 / 目标</th><th>等待条件</th><th>下一动作</th><th>期限 / 预算</th></tr></thead><tbody id="respBody">
      ${resp.length ? repeat(resp, x => x.id || JSON.stringify(x), r => html`<tr>
        <td><code class="mono">${r.id || "-"}</code><div>${managedBadge(r.state)}</div></td>
        <td>${r.task_id || "-"}<div class="muted">${r.attempt_id || ""} g=${r.generation == null ? "-" : r.generation} rv=${r.result_version || "-"}</div></td>
        <td>${r.owner || "-"} → ${r.target_session || "-"}</td>
        <td>${r.wait_kind || "-"}<div class="muted">${r.wait_condition || "-"}</div></td>
        <td>${r.next_action || "-"}<div class="muted">next_check ${ts(r.next_check_at)}</div></td>
        <td>${r.deadline == null ? "-" : ts(r.deadline)}<div class="muted">budget ${r.budget_used == null ? "-" : r.budget_used}/${r.max_budget == null ? "-" : r.max_budget} · rev ${r.revision == null ? "-" : r.revision}</div></td>
      </tr>`) : html`<tr><td colspan="6" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>

  </section>`;
}

function auditSection() {
  // list_audit 提供 id；无 id 的夹具回退含完整行字段与行号，避免同毫秒同实体不同动作的 key 重复混行。
  const audit = (state.audit || []).map((a, index) => ({...a, _i: index}));
  const auditKey = a => a.id != null ? String(a.id)
    : `${a._i}|${a.at || 0}|${a.action || ""}|${a.entity || ""}|${a.entity_id || ""}|${a.from_state || ""}|${a.to_state || ""}|${a.actor || ""}`;
  return html`<section>
    <h2>最近审计</h2>
    <div class="tablewrap"><table><thead><tr><th>时间</th><th>操作者</th><th>操作</th><th>对象</th><th>状态变化</th></tr></thead><tbody id="auditBody">
      ${audit.length ? repeat(audit, auditKey, a => html`<tr><td>${ts(a.at)}</td><td>${a.actor || "-"}</td><td>${a.action || "-"}</td><td>${a.entity || ""} ${a.entity_id || ""}</td><td>${a.from_state || ""} → ${a.to_state || ""}</td></tr>`) : html`<tr><td colspan="5" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>
  </section>`;
}

// R2 业务健康（全局只读投影；进程健康 ≠ 业务完成；运行身份只用 health.runtime）
function bizHealthSection() {
  const h = state.health;
  const body = h?.body;
  const tasks = (body?.tasks) || [];
  const proc = body?.process || {};
  const runtime = body?.runtime || {};
  const procLine = proc.healthy == null
    ? html`<span class="muted">进程健康未知</span>`
    : proc.healthy
      ? html`<span class="ok">进程正常</span> ${proc.connected != null ? `connected=${proc.connected}` : ""}${proc.heartbeat_age_s != null ? html` · 心跳 ${proc.heartbeat_age_s}s 前` : ""}`
      : html`<span class="bad">进程异常</span>`;
  const scopeNote = body?.coverage
    ? (body.coverage.complete ? "覆盖完整" : `部分覆盖 · 已显示 ${tasks.length} 项`)
    : "覆盖未知";
  const trow = t => {
    const c = t.control || {};
    const resp = (t.responsibilities || [])[0] || {};
    const bits = [
      c.project_intent ? `意图 ${c.project_intent}` : null,
      c.turn_held ? `hold${c.hold?.reason ? " " + c.hold.reason : ""}${c.hold?.held_at ? "（" + ts(c.hold.held_at) + "）" : ""}` : null,
      c.user_turn_active ? "回合中" : null,
      c.provider_gate ? `provider ${c.provider_gate}` : null,
    ].filter(Boolean).join(" · ");
    const ver = c.project_revision != null ? `rev ${c.project_revision}` : "";
    const ep = c.epoch != null ? `epoch ${c.epoch}` : "";
    return html`<tr>
      <td><a href=${`#/task/${encodeURIComponent(t.task_id || "")}`}><code class="mono">${t.task_id || "-"}</code></a><div class="muted">${t.project || ""}</div></td>
      <td>${badge(t.state)}${t.process_healthy != null ? html`<div class="muted">proc ${t.process_healthy ? "ok" : "down"}</div>` : nothing}</td>
      <td class="mono">${t.owner || "-"}</td>
      <td>${t.next_action || "-"}</td>
      <td>${resp.deadline != null ? ts(resp.deadline) : "-"}<div class="muted">next ${resp.next_check_at != null ? ts(resp.next_check_at) : "-"}</div></td>
      <td>${resp.budget_used != null ? `${resp.budget_used}/${resp.max_budget ?? "-"}` : "-"}</td>
      <td>${t.evidence_coverage === true ? html`<span class="ok">完整</span>` : t.evidence_coverage === false ? html`<span class="warn">不足</span>` : html`<span class="muted">-</span>`}</td>
      <td>${bits || html`<span class="muted">无控制记录</span>`}${ver || ep ? html`<div class="muted">${ver}${ver && ep ? " · " : ""}${ep}</div>` : nothing}</td>
    </tr>`;
  };
  const runtimeBits = [];
  if (runtime.release_id) runtimeBits.push(html`release ${runtime.release_id}`);
  if (runtime.manifest_verified === true) runtimeBits.push(html`<span class="ok">manifest 校验通过</span>`);
  else if (runtime.source) runtimeBits.push(html`<span class="muted">${runtime.source}</span>`);
  if (runtime.package_version) runtimeBits.push(html`包版本 ${runtime.package_version}`);
  return html`<section id="diag-bizhealth" aria-label="业务健康">
    <h2>业务健康</h2>
    <div class="note">任务完成以验收结论为准。 ${scopeNote}${state.healthErr ? html` <span class="bad">刷新失败，保留旧数据</span>` : nothing}</div>
    <div class="note">${procLine}</div>
    <div class="tablewrap"><table><thead><tr><th>任务</th><th>状态</th><th>负责人</th><th>下一动作</th><th>期限 / 下次检查</th><th>预算</th><th>证据</th><th>调度限制</th></tr></thead><tbody>
      ${tasks.length ? repeat(tasks, t => t.task_id || JSON.stringify(t), trow)
        : html`<tr><td colspan="8" class="muted">${h ? "当前无未完成业务健康记录。" : "健康数据未加载。"}</td></tr>`}
    </tbody></table></div>
    <div class="note" id="runtimeIdentity">运行版本 · ${runtimeBits.length ? runtimeBits : html`未知`}${runtime.email_default_enabled === false ? " · 邮件默认关闭" : ""}</div>
  </section>`;
}

// R2 本地人工待办：脱敏投影，只读；不 reconcile、不主动发邮件。
function incidentsSection() {
  const items = state.incidents || [];
  const st = state.incidentStatus || {};
  const coverage = st.coverage || {};
  const chTxt = st.channel === "not-configured"
    ? "未配置" : (st.channel || "-");
  return html`<section id="diag-incidents" aria-label="本地人工待办">
    <h2>本地人工待办</h2>
    <div class="muted" id="incidentMeta">已加载 ${items.length} / 共 ${st.total ?? "未知"} 条 · ${coverage.scope_complete === true ? "覆盖完整" : "部分覆盖或未知"} · 提醒渠道 ${chTxt} · 收件人 ${(st.recipient_refs || []).join(",") || "无"}${st.email_configured != null ? html` · 邮件${st.email_configured ? "已配置" : "未配置"}` : nothing}</div>
    ${coverage.invalid_records || coverage.unattributed_records ? html`<div class="warn">存在损坏或不可归属记录，不能据空列表认定无人工待办。</div>` : nothing}
    ${coverage.next_after_id ? html`<div class="note">后页游标：<code>${coverage.next_after_id}</code> · 可使用 incidents --after-id 查看后续记录</div>` : nothing}
    <div class="tablewrap"><table><thead><tr><th>ID / 类型</th><th>状态</th><th>负责人 / 任务</th><th>下一动作</th><th>期限 / 首次</th><th>通道</th></tr></thead><tbody>
      ${items.length ? repeat(items, i => i.incident_id || JSON.stringify(i), it => html`<tr>
        <td><code class="mono">${it.incident_id || "-"}</code><div class="muted">${it.incident_type || "-"} · ${it.source_kind || ""}${it.escalation_level ? ` · esc ${it.escalation_level}` : ""}</div></td>
        <td>${badge(it.state)}</td><td>${it.owner || "-"} → ${it.task_id || "-"}</td>
        <td>${it.next_action || "-"}</td>
        <td>${it.deadline ? ts(it.deadline) : ts(it.first_seen_at)}</td>
        <td>${it.channel || "-"}</td></tr>`) : html`<tr><td colspan="6" class="muted">${coverage.scope_complete === true ? "当前范围没有登记的待办" : "当前页无可读记录，完整范围尚未证明"}</td></tr>`}
    </tbody></table></div>
  </section>`;
}

export function renderDiagnostics(section) {
  const snap = state.snap || {};
  const ns = snap.notifications || [];
  const alerts = snap.alerts || [];
  const inputs = state.inputs || [];
  const watches = state.watches || [];
  const resp = state.resp || [];
  if (section) {
    const showAlerts = section === "alerts";
    const title = showAlerts ? "未关联任务的告警" : "其他负责人待办";
    return html`<div class="breadcrumb"><a href="#/">总览</a> / <a href="#/diagnostics">系统诊断</a></div>
      ${showAlerts ? alertSection(alerts.filter(a => !a.task_id && !a.attempt_id), title)
        : state.coordMeta ? respSection(resp.filter(r => !state.coordMap[r.owner]), title)
        : html`<h2>${title}</h2><p>无法读取会话归属。</p>`}`;
  }
  return html`<h2>系统诊断</h2>
    ${notifSection(ns)}
    ${alertSection(alerts)}
    ${respSection(resp)}
    ${incidentsSection()}
    ${gatesSection()}
    ${supSection()}
    ${leasesSection(snap)}
    ${sendSection()}
    ${managedSection(inputs, watches)}
    ${bizHealthSection()}
    ${auditSection()}`;
}

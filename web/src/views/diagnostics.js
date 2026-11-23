// 系统诊断：通知/告警表格、协调者 manual 解锁、Supervisor、租约退避、发送受管输入、
// 受管输入/白名单观察、后续责任、最近审计。纯 Lit html 模板。
// 交互经 data 属性冒泡由 app 统一委托（ackchecked/ackalerts/unlock/supstart/supstop/data-managed）。
import {html, nothing} from "lit";
import {repeat} from "lit/directives/repeat.js";
import {state, ro} from "../state.js";
import {ts, badge, gateBadge, fmtAge, unlockActionFor} from "../ui.js";

const MANAGED_VALUES = {
  ok: ["delivered", "succeeded", "acknowledged", "admitted", "recorded"],
  blocked: ["prepared", "possible", "pending", "unknown", "unacknowledged", "not-applicable"],
  bad: ["cancelled", "failed", "interrupted"],
};
function managedBadge(v) {
  if (v == null || v === "") return html`<span class="muted">-</span>`;
  const s = String(v);
  if (MANAGED_VALUES.ok.includes(s)) return html`<span class="badge b-valid">${s}</span>`;
  if (MANAGED_VALUES.blocked.includes(s)) return html`<span class="badge b-blocked">${s}</span>`;
  if (MANAGED_VALUES.bad.includes(s)) return html`<span class="badge b-expired">${s}</span>`;
  return html`<span class="badge">${s}</span>`;
}
const managedSession = i => i.session_id || i.session || "-";
function watchEnabledBadge(v) {
  if (v === true) return html`<span class="badge b-valid">enabled</span>`;
  if (v === false) return html`<span class="badge b-expired">disabled</span>`;
  return html`<span class="muted">-</span>`;
}
function obsTrustBadge(v) {
  if (v === true) return html`<span class="badge b-valid">trusted</span>`;
  if (v === false) return html`<span class="badge b-expired">untrusted</span>`;
  return html`<span class="badge b-blocked">unknown</span>`;
}
function watchObservation(w) {
  const obs = w && w.last_observation;
  if (!obs || typeof obs !== "object" || !Object.keys(obs).length) return html`<span class="muted">unknown / 未观察</span>`;
  return html`pending ${obs.pending == null ? "-" : obs.pending} · age ${obs.oldest_age_ms == null ? "-" : fmtAge(obs.oldest_age_ms)} ${obsTrustBadge(obs.trusted)}${obs.aged_out === true ? html` <span class="badge b-blocked">aged_out</span>` : nothing}${obs.stale === true ? html` <span class="badge b-blocked">stale</span>` : nothing}${obs.blocking_reason != null ? html` <span class="muted">blocking ${obs.blocking_reason}</span>` : nothing}${w.last_observed_at ? html`<div class="muted">observed ${ts(w.last_observed_at)}</div>` : nothing}${obs.next_action ? html`<div class="muted">next ${obs.next_action}</div>` : nothing}`;
}

function notifSection(ns) {
  return html`<section id="diag-notif" aria-label="通知与告警">
    <h2>通知（回执与 ACK 分开记账：ACK 只关闭收件责任，不等于任务验收）</h2>
    <div class="row" style="margin-bottom:8px">
      <span class="muted" id="notifMeta">当前快照 ${ns.length} 条（pending/inflight/sent 上限 50；sent_unacked 未含）</span>
      <span class="spacer"></span>
      <button id="ackCheckedBtn" data-act="ackchecked" class="primary" ?disabled=${ro()}>ACK 勾选通知</button>
    </div>
    <div class="tablewrap"><table id="notifTable"><thead><tr><th>ID / Kind</th><th>State</th><th>Target</th><th>摘要</th><th>Last error</th></tr></thead><tbody>
      ${ns.length ? repeat(ns, n => n.id, n => html`<tr><td><label class="inline"><input type="checkbox" class="noteck" data-id=${n.id} checked> <code class="mono">${n.id}</code></label><div class="muted">${n.kind}</div></td><td>${badge(n.state)}</td><td><code class="mono">${n.target_session || "-"}</code>${n.task_id || n.attempt_id ? html`<div class="muted">${n.task_id || ""} ${n.attempt_id || ""}</div>` : nothing}</td><td>${n.summary || ""}${n.missing_deliverables && n.missing_deliverables.length ? html`<div class="bad">missing ${n.missing_deliverables.join("、")}</div>` : nothing}</td><td class="mono">${n.last_error || "-"}</td></tr>`) : html`<tr><td colspan="5" class="muted">(none)</td></tr>`}
    </tbody></table></div>
    <div class="note">批量 ACK 只处理当前勾选且可见的通知；勾选数量在确认框中列出具体 ID 集合。read_only 模式禁用。</div>
  </section>`;
}

function alertSection(alerts) {
  return html`<section id="diag-alerts" aria-label="告警">
    <h2>告警（无法归属到项目的全局故障独立可见）</h2>
    <div class="row" style="margin-bottom:8px">
      <span class="muted" id="alertMeta">当前快照 ${alerts.length} 条新告警（上限 50）</span>
      <span class="spacer"></span>
      <button id="ackAlertsBtn" data-act="ackalerts" class="primary" ?disabled=${ro()}>ACK 勾选告警</button>
    </div>
    <div class="tablewrap"><table id="alertTable"><thead><tr><th>ID</th><th>级别</th><th>标题</th><th>归属</th><th>详情</th></tr></thead><tbody>
      ${alerts.length ? repeat(alerts, a => a.id, a => html`<tr><td><label class="inline"><input type="checkbox" class="alck" data-id=${a.id} checked> <code class="mono">${a.id}</code></label></td><td>${a.severity || ""}</td><td>${badge(a.kind)} ${a.title || ""}</td><td>${a.task_id || a.attempt_id ? html`<span class="muted">task ${a.task_id || ""} attempt ${a.attempt_id || ""}</span>` : html`<span class="warn">无归属（全局）</span>`}</td><td><details><summary class="muted">查看原文</summary><pre>${a.detail || ""}</pre></details></td></tr>`) : html`<tr><td colspan="5" class="muted">(none)</td></tr>`}
    </tbody></table></div>
    <div class="note">只 ACK 勾选且可见的告警；详情可展开阅读原文。read_only 模式禁用。</div>
  </section>`;
}

function gatesSection() {
  const coords = state.coordList.map(sid => state.coordMap[sid]).filter(Boolean);
  return html`<section id="diag-gates" aria-label="协调者解锁">
    <h2>协调者解锁（manual 与 provider 恢复调度均需原因 + UNLOCK 确认）</h2>
    <div id="gatesRoot"><div class="tablewrap"><table><thead><tr><th>Session</th><th>闸门</th><th></th></tr></thead><tbody>
      ${coords.length ? repeat(coords, c => c.session, c => {
        const ua = unlockActionFor(c);
        return html`<tr><td><a href=${`#/coordinator/${encodeURIComponent(c.session)}`}><code class="mono">${c.session}</code></a></td><td>${(c.gates || []).length ? c.gates.map(gateBadge) : html`<span class="muted">无闸门</span>`}</td><td>${ua ? html`<button class="danger" data-act="unlock" data-sid=${c.session} data-project=${ua.project} data-allow-provider=${ua.allowProvider ? "1" : "0"} ?disabled=${ro()}>${ua.label}</button>` : html`<span class="muted">无可执行操作</span>`}</td></tr>`;
      }) : html`<tr><td colspan="3" class="muted">(none)</td></tr>`}
    </tbody></table></div></div>
    <div class="note">manual 闸门 → 解锁 manual（清除人工中断）；provider 冷却/耗尽/半开未知 → 恢复调度（把闸门复位为 probe-ready，新错误仍会重新冷却）。原因必填、须输入 UNLOCK 确认；read_only 模式禁用。</div>
  </section>`;
}

function supSection() {
  return html`<section id="diag-supervisor" aria-label="Supervisor 生命周期">
    <h2>Supervisor 生命周期</h2>
    <div class="op-panel" id="supRoot"><button class="primary" data-act="supstart" ?disabled=${ro()}>启动 Supervisor</button><button class="danger" data-act="supstop" ?disabled=${ro()}>停止 Supervisor</button></div>
    <div class="note">启动/停止需输入 START / STOP 确认；read_only 模式下禁用。</div>
  </section>`;
}

function leasesSection(snap) {
  const leases = snap.leases || [];
  const back = snap.provider_backoffs || {};
  const gates = snap.target_gates || [];
  return html`<section id="diag-leases" aria-label="租约与退避">
    <h2>协调者租约 / Provider 退避</h2>
    <div id="meta"><div class="note">原始台账事实（租约有效不代表在线；provider 退避为只读）</div>
      <h3>Leases</h3><pre>${JSON.stringify(leases, null, 1)}</pre>
      <h3>Provider backoff</h3><pre>${JSON.stringify(back, null, 1)}</pre>
      <h3>Target gates</h3><pre>${JSON.stringify(gates, null, 1)}</pre>
    </div>
    <div class="note">展示原始台账事实；租约有效不代表 session 在线，页面在调度者卡片上单独显示在线状态。</div>
  </section>`;
}

function sendSection() {
  const r = ro();
  return html`<section id="diag-sup-input" aria-label="发送受管输入">
    <h2>发送受管输入（execute / park / record）</h2>
    <div id="sendFormHost"><form id="sendForm" data-form="send" class="row">
      <label>session<input type="text" name="session" placeholder="ses_…" required ?disabled=${r}></label>
      <label>intent<input type="text" name="intent" value="execute" required ?disabled=${r}></label>
      <label>文本<input type="text" name="text" required ?disabled=${r}></label>
      <label>owner<input type="text" name="owner" required ?disabled=${r}></label>
      <label>request_id<input type="text" name="request_id" required ?disabled=${r}></label>
      <label>project<input type="text" name="project" ?disabled=${r}></label>
      <label>task<input type="text" name="task" ?disabled=${r}></label>
      <label>attempt<input type="text" name="attempt" ?disabled=${r}></label>
      <label>target_session<input type="text" name="target_session" ?disabled=${r}></label>
      <button class="primary" type="submit" ?disabled=${r}>发送</button>
    </form></div>
    <div class="note">写请求经既有守卫（X-OC-Orch-Web + Origin + read_only）；read_only 模式下禁用。受理 ≠ 投递 ≠ 执行 ≠ 业务 ACK。</div>
  </section>`;
}

function managedSection(inputs, watches) {
  const r = ro();
  return html`<section id="diag-managed" aria-label="受管输入与白名单">
    <h2>受管输入 / 白名单观察</h2>
    <div class="muted" id="managedMeta">${inputs.length} 个输入 · ${watches.length} 条白名单（列表不含正文；受理≠投递≠执行≠业务ACK）</div>
    <div class="tablewrap"><table><thead><tr><th>Input / Message</th><th>Session</th><th>Owner</th><th>Intent</th><th>受理</th><th>投递</th><th>执行</th><th>业务ACK</th><th>年龄 / 下一动作</th><th>操作</th></tr></thead><tbody id="managedBody">
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
        <td>${r ? "-" : html`${["flush", "cancel"].map(a => html`<button data-managed=${a} data-id=${i.input_id} data-rev=${i.revision == null ? "" : i.revision}>${a}</button>`)}${i.business_ack !== "acknowledged" ? html`<button class="primary" data-managed="ack" data-id=${i.input_id} data-rev=${i.revision == null ? "" : i.revision}>ack</button>` : nothing}`}</td>
      </tr>`) : html`<tr><td colspan="10" class="muted">(none)</td></tr>`}
    </tbody></table></div>
    <div class="muted" style="margin-top:8px">白名单（仅显式登记分支；观察只补事实/告警，不自动接管；本页只读，不发远端 observe/reconcile）</div>
    <div class="tablewrap"><table><thead><tr><th>Session</th><th>Owner</th><th>Project</th><th>状态</th><th>Max-Pending-Age</th><th>Revision / Pending</th><th>最近观察</th><th>下一动作</th></tr></thead><tbody id="watchBody">
      ${watches.length ? repeat(watches, w => w.session_id || w.session || JSON.stringify(w), w => html`<tr>
        <td><code class="mono">${w.session_id || w.session || "-"}</code></td>
        <td>${w.owner || "-"}</td>
        <td>${w.project || "-"}</td>
        <td>${watchEnabledBadge(w.enabled)}</td>
        <td>${w.max_pending_age_ms == null ? "-" : w.max_pending_age_ms + " ms"}</td>
        <td>${w.revision == null ? "-" : w.revision}${w.pending_count != null ? html`<div class="muted">pending ${w.pending_count}</div>` : nothing}</td>
        <td>${watchObservation(w)}</td>
        <td>${w.next_action || "-"}</td>
      </tr>`) : html`<tr><td colspan="8" class="muted">(none)</td></tr>`}
    </tbody></table></div>
    <div class="note">输入列表只读展示；flush / cancel / ack 在下方操作区逐行执行（需输入 ID 确认与 revision），不会顺带处理未列出项。</div>
  </section>`;
}

function respSection(resp) {
  return html`<section>
    <h2>后续责任（谁在等待什么）</h2>
    <div class="muted" id="respMeta">${resp.length} 条未关闭后续责任（当前读取 ≤200，超限不含；ACK 不关闭责任；Task accepted 仍须 verify）</div>
    <div class="tablewrap"><table><thead><tr><th>ID / 状态</th><th>Task / Attempt</th><th>Owner → Target</th><th>等待条件</th><th>下一动作</th><th>期限 / 预算</th></tr></thead><tbody id="respBody">
      ${resp.length ? repeat(resp, x => x.id || JSON.stringify(x), r => html`<tr>
        <td><code class="mono">${r.id || "-"}</code><div>${managedBadge(r.state)}</div></td>
        <td>${r.task_id || "-"}<div class="muted">${r.attempt_id || ""} g=${r.generation == null ? "-" : r.generation} rv=${r.result_version || "-"}</div></td>
        <td>${r.owner || "-"} → ${r.target_session || "-"}</td>
        <td>${r.wait_kind || "-"}<div class="muted">${r.wait_condition || "-"}</div></td>
        <td>${r.next_action || "-"}<div class="muted">next_check ${ts(r.next_check_at)}</div></td>
        <td>${r.deadline == null ? "-" : ts(r.deadline)}<div class="muted">budget ${r.budget_used == null ? "-" : r.budget_used}/${r.max_budget == null ? "-" : r.max_budget} · rev ${r.revision == null ? "-" : r.revision}</div></td>
      </tr>`) : html`<tr><td colspan="6" class="muted">(none)</td></tr>`}
    </tbody></table></div>
    <div class="note">责任与通知 ACK 分开记账：ACK 只关闭收件责任；Task accepted 仍须 verify。本页只读。</div>
  </section>`;
}

function auditSection() {
  // list_audit 提供 id；无 id 的夹具回退含完整行字段与行号，避免同毫秒同实体不同动作的 key 重复混行。
  const audit = (state.audit || []).map((a, index) => ({...a, _i: index}));
  const auditKey = a => a.id != null ? String(a.id)
    : `${a._i}|${a.at || 0}|${a.action || ""}|${a.entity || ""}|${a.entity_id || ""}|${a.from_state || ""}|${a.to_state || ""}|${a.actor || ""}`;
  return html`<section>
    <h2>最近审计</h2>
    <div class="tablewrap"><table><thead><tr><th>时间</th><th>Actor</th><th>Action</th><th>Entity</th><th>From → To</th></tr></thead><tbody id="auditBody">
      ${audit.length ? repeat(audit, auditKey, a => html`<tr><td>${ts(a.at)}</td><td>${a.actor || "-"}</td><td>${a.action || "-"}</td><td>${a.entity || ""} ${a.entity_id || ""}</td><td>${a.from_state || ""} → ${a.to_state || ""}</td></tr>`) : html`<tr><td colspan="5" class="muted">(none)</td></tr>`}
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
      ? html`<span class="ok">进程 healthy</span> ${proc.connected != null ? `connected=${proc.connected}` : ""}${proc.heartbeat_age_s != null ? html` · 心跳 ${proc.heartbeat_age_s}s 前` : ""}`
      : html`<span class="bad">进程 unhealthy</span>`;
  const scopeNote = body?.coverage
    ? (body.coverage.complete ? "覆盖完整" : `部分覆盖（当前显示 ${tasks.length} 项）`)
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
    <h2>业务健康（只读投影；进程健康 ≠ 业务进展/完成）</h2>
    <div class="note">${body?.semantics || ""}（${scopeNote}）${state.healthErr ? html` <span class="bad">刷新失败，保留旧数据</span>` : nothing}</div>
    <div class="note">${procLine}</div>
    <div class="tablewrap"><table><thead><tr><th>Task</th><th>状态</th><th>Owner</th><th>下一动作</th><th>期限 / 下次检查</th><th>预算</th><th>证据</th><th>控制（意图/hold/回合/provider）</th></tr></thead><tbody>
      ${tasks.length ? repeat(tasks, t => t.task_id || JSON.stringify(t), trow)
        : html`<tr><td colspan="8" class="muted">${h ? "当前无未完成业务健康记录。" : "健康数据未加载。"}</td></tr>`}
    </tbody></table></div>
    <div class="note" id="runtimeIdentity">运行身份：${runtimeBits.length ? runtimeBits : html`-`}。邮件提醒默认关闭、native 对话 hook 未启用（均以 health.runtime 返回为准，不读取/信任 Git 版本自报）。</div>
  </section>`;
}

// R2 本地人工待办：脱敏投影，只读；不 reconcile、不主动发邮件。
function incidentsSection() {
  const items = state.incidents || [];
  const st = state.incidentStatus || {};
  const coverage = st.coverage || {};
  const chTxt = st.channel === "not-configured"
    ? "not-configured（邮件默认关闭，不发信）" : (st.channel || "-");
  return html`<section id="diag-incidents" aria-label="本地人工待办">
    <h2>本地人工待办（incidents；只读）</h2>
    <div class="muted" id="incidentMeta">已加载 ${items.length} / 持久候选 ${st.total ?? "未知"} 条 · ${coverage.scope_complete === true ? "覆盖完整" : "部分覆盖或未知"} · channel=${chTxt} · recipient_refs=${(st.recipient_refs || []).join(",") || "无"}${st.email_configured != null ? html` · email_configured=${st.email_configured}` : nothing}</div>
    ${coverage.invalid_records || coverage.unattributed_records ? html`<div class="warn">存在损坏或不可归属记录，不能据空列表认定无人工待办。</div>` : nothing}
    ${coverage.next_after_id ? html`<div class="note">后页游标：<code>${coverage.next_after_id}</code>（CLI incidents --after-id 续读，不推进自动扫描游标）</div>` : nothing}
    <div class="tablewrap"><table><thead><tr><th>ID / 类型</th><th>状态</th><th>Owner → Task</th><th>下一动作</th><th>期限 / 首次</th><th>通道</th></tr></thead><tbody>
      ${items.length ? repeat(items, i => i.incident_id || JSON.stringify(i), it => html`<tr>
        <td><code class="mono">${it.incident_id || "-"}</code><div class="muted">${it.incident_type || "-"} · ${it.source_kind || ""}${it.escalation_level ? ` · esc ${it.escalation_level}` : ""}</div></td>
        <td>${badge(it.state)}</td><td>${it.owner || "-"} → ${it.task_id || "-"}</td>
        <td>${it.next_action || "-"}</td>
        <td>${it.deadline ? ts(it.deadline) : ts(it.first_seen_at)}</td>
        <td>${it.channel || "-"}</td></tr>`) : html`<tr><td colspan="6" class="muted">${coverage.scope_complete === true ? "当前范围无登记记录（不是业务完成证明）" : "当前页无可读记录，完整范围尚未证明"}</td></tr>`}
    </tbody></table></div>
    <div class="note">仅展示本地可行动待办与通道状态；不调用 reconcile、不主动发送邮件。邮件启用以独立授权为准，health.runtime.email_default_enabled=false 不代表执行器进程级状态，本页不自行声称已启用。</div>
  </section>`;
}

export function renderDiagnostics() {
  const snap = state.snap || {};
  const ns = snap.notifications || [];
  const alerts = snap.alerts || [];
  const inputs = state.inputs || [];
  const watches = state.watches || [];
  const resp = state.resp || [];
  return html`<h2>系统诊断（只读区为表格；写操作为独立受控表单）</h2>
    ${notifSection(ns)}
    ${alertSection(alerts)}
    ${gatesSection()}
    ${supSection()}
    ${leasesSection(snap)}
    ${sendSection()}
    ${managedSection(inputs, watches)}
    ${respSection(resp)}
    ${incidentsSection()}
    ${bizHealthSection()}
    ${auditSection()}`;
}

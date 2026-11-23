import {html, nothing} from "lit";
import {state} from "./state.js";

export const $ = id => document.getElementById(id);
export const ts = value => value ? new Date(value).toLocaleString("zh-CN", {hour12: false}) : "-";
const labels = new Map(Object.entries({
  running: "执行中", draft: "草稿", pending: "待处理", assigned: "已分配",
  paused: "已暂停", blocked: "受阻", stalled: "进展停滞", accepted: "已验收",
  cancelled: "已取消", rejected: "未通过", result_pending: "待收集或验收",
  result_received: "已收到结果", result_collected: "已收集结果", result_missing: "缺少结果",
  terminal_succeeded: "执行成功", terminal_failed: "执行失败", terminal_interrupted: "执行中断",
  verification_failed: "验收未通过", pass: "通过", fail: "未通过", unknown: "未知",
  dispatched: "已派单", queued: "已排队", sent: "已发送", inflight: "发送中", acked: "已确认收到",
  creating: "创建中", dispatch_unknown: "派单结果未知", blocked_permission: "等待权限",
  blocked_form: "等待填写表单", provider_retry: "等待服务商重试", idle_unconfirmed: "空闲待确认",
  stop_requested: "已请求停止", lost: "失去联系", awaiting_review: "待复核", processing: "处理中",
  active: "活动中", ready: "就绪", dispatching: "派单中", working: "执行中",
  waiting_human: "等待人工处理", waiting_dependency: "等待依赖", closed: "已关闭",
  delivered: "已送达", succeeded: "执行成功", acknowledged: "已确认", unacknowledged: "待确认",
  admitted: "已受理", recorded: "已记录", failed: "失败", interrupted: "已中断",
  prepared: "已准备", possible: "可能已执行", "not-applicable": "不适用",
  "manual-blocked": "等待人工解锁", exhausted: "重试次数已用完", cooling: "冷却中",
  "probe-unknown": "探测结果未知", "probe-ready": "等待探测", "blocked-unknown": "阻塞原因未知",
}));
export const displayStatus = value => labels.get(value) ?? (value || "未知");
export const badge = value => html`<span class=${`badge b-${value || "unknown"}`}>${displayStatus(value)}</span>`;
const runLabels = new Map([["running", "运行中"], ["inactive", "空闲"], ["unknown", "状态未知"]]);
export function sessionRunStatus(c, stale) {
  const status = stale || c.missing_snapshot || !runLabels.has(c.session_run_state)
    ? "unknown" : c.session_run_state;
  return html`<span class=${`presence session-run run-${status}`}>${runLabels.get(status)}</span>`;
}
export function processStatus(s, stale) {
  if (stale) return {text: "待更新", tone: "warn"};
  if (s.alive === false) return {text: "未检测到进程", tone: "bad"};
  if (s.healthy === true) return {text: "正常", tone: "ok"};
  if (s.alive === true && s.healthy === false) return {text: "心跳异常", tone: "warn"};
  if (s.healthy === false) return {text: "异常", tone: "bad"};
  return {text: "状态未知", tone: "muted"};
}

export function connectionStatus(s, stale) {
  if (stale) return "待更新";
  if (s.alive === false) return "未连接";
  if (s.healthy !== true) return "未知";
  const stream = s.stream || {};
  if (stream.backoff > 0) return "正在重试";
  // connected 由 last_connected_at 派生。旧心跳可能只保存了过去成功连接的事实。
  if (s.connected === true) return stream.backoff === 0 ? "已连接" : "曾建立连接";
  if (s.connected === false && stream.connects > 0) return "连接中";
  return s.connected === false ? "尚未连接" : "未知";
}
export function fmtAge(ms) {
  if (ms == null || isNaN(Number(ms))) return "-";
  const s = Math.max(0, Math.floor(Number(ms) / 1000));
  if (s < 60) return `${s} 秒`;
  if (s < 3600) return `${Math.floor(s / 60)} 分钟`;
  if (s < 86400) return `${Math.floor(s / 3600)} 小时`;
  return `${Math.floor(s / 86400)} 天`;
}
let toastTimer;
export function toast(message) {
  const node = $("toast");
  if (!node) return;
  node.textContent = message;
  node.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => node.classList.remove("show"), 2000);
}
export async function copyText(text) {
  if (!text) return;
  try {
    if (navigator.clipboard?.writeText) await navigator.clipboard.writeText(text);
    else {
      const active = document.activeElement;
      const node = document.createElement("textarea");
      node.value = text;
      node.style.cssText = "position:fixed;opacity:0";
      document.body.append(node);
      node.select();
      try { if (!document.execCommand("copy")) throw new Error("copy"); }
      finally { node.remove(); active?.focus({preventScroll: true}); }
    }
    toast("已复制 " + text.slice(0, 40));
  } catch { toast("复制失败，请手动选择"); }
}
// 记录“存在未提交表单/脏值”：不暂停自动刷新，仅用于提示与健康条。
export function markDirty() {
  state.dirtyForm = state.verifyForms.size > 0 || !!document.querySelector('[data-dirty="true"]');
}
export function gateBadge(gate) {
  return gate?.state ? html`<span class=${`badge b-${gate.state === "manual-blocked" ? "expired" : "blocked"}`}>${displayStatus(gate.state)}</span>` : nothing;
}
// 可解锁动作判定：manual 闸门 → 解锁 manual；provider 冷却/耗尽/半开未知 →
// 恢复调度（授权 operator 入口，需原因 + UNLOCK 确认）。probe-ready/探测中无动作。
export function unlockActionFor(coord) {
  const project = (coord.leases || [])[0]?.project || (coord.projects || [])[0];
  if (!project) return null;
  const manual = (coord.gates || []).find(g => g.state === "manual-blocked");
  if (manual) return { label: "解除人工阻塞", project, allowProvider: false };
  const blocking = (coord.gates || []).find(g => g.source === "provider" && ["blocked", "exhausted", "probe-unknown"].includes(g.state));
  if (blocking) return { label: "恢复调度", project, allowProvider: true };
  return null;
}

export function identity(coordinator) {
  const confirmed = coordinator.identity === "confirmed" || (coordinator.sources || []).some(s => s.startsWith("task:") || s.startsWith("lease:"));
  const notifyOnly = !confirmed && (coordinator.sources || []).length > 0 && coordinator.sources.every(s => s.startsWith("notify:"));
  return {
    badge: confirmed ? html`<span class="badge b-valid">已关联调度者</span>` : notifyOnly ? html`<span class="badge b-blocked">仅通知关联 · 调度者角色未确认</span>` : html`<span class="badge">身份未知</span>`,
    name: coordinator.title?.trim() || (notifyOnly ? "通知关联会话" : coordinator.session || "未命名会话"),
  };
}
// 复制按钮只带 data-copy，由 app 根节点唯一委托处理（去掉旧的 @click，避免重复绑定）。
export const copyButton = (id, label = "复制 ID") =>
  html`<button class="copy" data-copy=${id} aria-label=${label}>${label}</button>`;
export const busy = key => state.busyActions.has(key);
export const verifySubmissionNote = "按标准验收，提交时会核对交付证据。";

/* ============ 诊断表格辅助（沿用旧页语义） ============ */
const MANAGED_VALUES = {
  ok: ["delivered", "succeeded", "acknowledged", "admitted", "recorded"],
  blocked: ["prepared", "possible", "pending", "unknown", "unacknowledged", "not-applicable"],
  bad: ["cancelled", "failed", "interrupted"],
};
export function managedBadge(value) {
  if (value == null || value === "") return html`<span class="muted">-</span>`;
  const s = String(value);
  if (MANAGED_VALUES.ok.includes(s)) return html`<span class="badge b-valid">${displayStatus(s)}</span>`;
  if (MANAGED_VALUES.blocked.includes(s)) return html`<span class="badge b-blocked">${displayStatus(s)}</span>`;
  if (MANAGED_VALUES.bad.includes(s)) return html`<span class="badge b-expired">${displayStatus(s)}</span>`;
  return html`<span class="badge">${displayStatus(s)}</span>`;
}
export const managedAge = fmtAge;
export function watchEnabledBadge(v) {
  if (v === true) return html`<span class="badge b-valid">已启用</span>`;
  if (v === false) return html`<span class="badge b-expired">已停用</span>`;
  return html`<span class="muted">-</span>`;
}
export function obsTrustBadge(v) {
  if (v === true) return html`<span class="badge b-valid">可信</span>`;
  if (v === false) return html`<span class="badge b-expired">未获信任</span>`;
  return html`<span class="badge b-blocked">未知</span>`;
}
export function watchObservation(w) {
  const obs = w.last_observation;
  if (!obs || typeof obs !== "object" || !Object.keys(obs).length) return html`<span class="muted">尚无观察记录</span>`;
  const bits = [
    html`待处理 ${obs.pending == null ? "-" : obs.pending}`,
    html` · 最长等待 ${obs.oldest_age_ms == null ? "-" : fmtAge(obs.oldest_age_ms)}`,
    obsTrustBadge(obs.trusted),
  ];
  if (obs.aged_out === true) bits.push(html`<span class="badge b-blocked">已超时</span>`);
  if (obs.stale === true) bits.push(html`<span class="badge b-blocked">已过期</span>`);
  if (obs.blocking_reason != null) bits.push(html`<span class="muted">受阻原因 ${obs.blocking_reason}</span>`);
  if (w.last_observed_at) bits.push(html`<div class="muted">观察时间 ${ts(w.last_observed_at)}</div>`);
  if (obs.next_action) bits.push(html`<div class="muted">下一步 ${obs.next_action}</div>`);
  return html`${bits}`;
}
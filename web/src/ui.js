import {html, nothing} from "lit";
import {state} from "./state.js";

export const $ = id => document.getElementById(id);
export const ts = value => value ? new Date(value).toLocaleString() : "-";
export const badge = value => html`<span class=${`badge b-${value || "unknown"}`}>${value || "unknown"}</span>`;
export function fmtAge(ms) {
  if (ms == null || isNaN(Number(ms))) return "-";
  const s = Math.max(0, Math.floor(Number(ms) / 1000));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor(s / 60) % 60}m`;
  return `${Math.floor(s / 86400)}d`;
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
  return gate?.state ? html`<span class=${`badge b-${gate.state === "manual-blocked" ? "expired" : "blocked"}`}>闸门 ${gate.state}${gate.source ? " / " + gate.source : ""}</span>` : nothing;
}
// 可解锁动作判定：manual 闸门 → 解锁 manual；provider 冷却/耗尽/半开未知 →
// 恢复调度（授权 operator 入口，需原因 + UNLOCK 确认）。probe-ready/探测中无动作。
export function unlockActionFor(coord) {
  const project = (coord.leases || [])[0]?.project || (coord.projects || [])[0];
  if (!project) return null;
  const manual = (coord.gates || []).find(g => g.state === "manual-blocked");
  if (manual) return { label: "解锁 manual", project, allowProvider: false };
  const blocking = (coord.gates || []).find(g => g.source === "provider" && ["blocked", "exhausted", "probe-unknown"].includes(g.state));
  if (blocking) return { label: "恢复调度", project, allowProvider: true };
  return null;
}

export function identity(coordinator) {
  const confirmed = coordinator.identity === "confirmed" || (coordinator.sources || []).some(s => s.startsWith("task:") || s.startsWith("lease:"));
  const notifyOnly = !confirmed && (coordinator.sources || []).length > 0 && coordinator.sources.every(s => s.startsWith("notify:"));
  return {
    badge: confirmed ? html`<span class="badge b-valid">已关联调度者</span>` : notifyOnly ? html`<span class="badge b-blocked">仅通知关联 · 调度者角色未确认</span>` : html`<span class="badge">身份未知</span>`,
    name: coordinator.title?.trim() || (notifyOnly ? "通知关联会话（未确认调度者）" : confirmed ? "未命名调度者" : "身份未知会话"),
  };
}
// 复制按钮只带 data-copy，由 app 根节点唯一委托处理（去掉旧的 @click，避免重复绑定）。
export const copyButton = (id, label = "复制 ID") =>
  html`<button class="copy" data-copy=${id} aria-label=${label}>${label}</button>`;
export const busy = key => state.busyActions.has(key);
export const verifySubmissionNote = "提交按后端标准验收（force=false，不宣称人工例外）；pass 时 reason 仅作备注，fail 时 reason 必填。";

/* ============ 诊断表格辅助（沿用旧页语义） ============ */
const MANAGED_VALUES = {
  ok: ["delivered", "succeeded", "acknowledged", "admitted", "recorded"],
  blocked: ["prepared", "possible", "pending", "unknown", "unacknowledged", "not-applicable"],
  bad: ["cancelled", "failed", "interrupted"],
};
export function managedBadge(value) {
  if (value == null || value === "") return html`<span class="muted">-</span>`;
  const s = String(value);
  if (MANAGED_VALUES.ok.includes(s)) return html`<span class="badge b-valid">${s}</span>`;
  if (MANAGED_VALUES.blocked.includes(s)) return html`<span class="badge b-blocked">${s}</span>`;
  if (MANAGED_VALUES.bad.includes(s)) return html`<span class="badge b-expired">${s}</span>`;
  return html`<span class="badge">${s}</span>`;
}
export const managedAge = fmtAge;
export function watchEnabledBadge(v) {
  if (v === true) return html`<span class="badge b-valid">enabled</span>`;
  if (v === false) return html`<span class="badge b-expired">disabled</span>`;
  return html`<span class="muted">-</span>`;
}
export function obsTrustBadge(v) {
  if (v === true) return html`<span class="badge b-valid">trusted</span>`;
  if (v === false) return html`<span class="badge b-expired">untrusted</span>`;
  return html`<span class="badge b-blocked">unknown</span>`;
}
export function watchObservation(w) {
  const obs = w.last_observation;
  if (!obs || typeof obs !== "object" || !Object.keys(obs).length) return html`<span class="muted">unknown / 未观察</span>`;
  const bits = [
    html`pending ${obs.pending == null ? "-" : obs.pending}`,
    html`age ${obs.oldest_age_ms == null ? "-" : fmtAge(obs.oldest_age_ms)}`,
    obsTrustBadge(obs.trusted),
  ];
  if (obs.aged_out === true) bits.push(html`<span class="badge b-blocked">aged_out</span>`);
  if (obs.stale === true) bits.push(html`<span class="badge b-blocked">stale</span>`);
  if (obs.blocking_reason != null) bits.push(html`<span class="muted">blocking ${obs.blocking_reason}</span>`);
  if (w.last_observed_at) bits.push(html`<div class="muted">observed ${ts(w.last_observed_at)}</div>`);
  if (obs.next_action) bits.push(html`<div class="muted">next ${obs.next_action}</div>`);
  return html`${bits}`;
}
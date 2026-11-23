// 仅显式提交的非密钥策略意图，保存在当前窗口sessionStorage以跨reload保原ID。
// 不在加载时访问网络，不把本地缓存当作执行/生效证明。
import {state} from "./state.js";

const prefix = "oc-orch-policy-pending:v1:";

export function pendingPolicy(key) {
  if (state.policyOperations.has(key)) return state.policyOperations.get(key);
  try {
    const raw = window.sessionStorage.getItem(prefix + key);
    if (!raw) return null;
    const value = JSON.parse(raw);
    if (typeof value?.body?.operation_id !== "string" || `${value.scope}:${value.target}` !== key) {
      return {body: {operation_id: "invalid-local-pending-record"}, invalid: true};
    }
    return value;
  } catch { return {body: {operation_id: "pending-record-unreadable"}, invalid: true}; }
}

export function savePendingPolicy(key, value) {
  // 无法持久化则不发送：不能在reload后丢失未知请求身份，再自动造新操作。
  window.sessionStorage.setItem(prefix + key, JSON.stringify(value));
  state.policyOperations.set(key, value);
}

export function clearPendingPolicy(key) {
  window.sessionStorage.removeItem(prefix + key);
  state.policyOperations.delete(key);
}

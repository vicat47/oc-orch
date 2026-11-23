// 指定显式补充入口；先持久保存原意图，unknown只观察，不自动重投。
import {api, post} from "./api.js";
import {ro, state, publish} from "./state.js";
import {toast} from "./ui.js";

const OPERATOR_TURN_STORAGE = "oc-orch.operator-turn.v1";
const records = () => {
  const value = JSON.parse(window.localStorage.getItem(OPERATOR_TURN_STORAGE) || "[]");
  if (!Array.isArray(value) || value.some(e => !e?.body?.operation_id || !e.body.project || !e.body.session_id)) throw new Error("原意图台账损坏，不猜测/覆盖");
  return value;
};
const save = entries => window.localStorage.setItem(OPERATOR_TURN_STORAGE, JSON.stringify(entries));

export async function conversationAction(element, observe = false) {
  const project = element.dataset.project, sid = element.dataset.sid;
  const key = `conversation:${sid}:${project}`;
  if (ro() || !project || !sid || state.busyActions.has(key)) return;
  state.busyActions.add(key); publish();
  let entries, sending;
  try {
    entries = records();
    if (observe) {
      const candidates = entries.filter(e => e.body.project === project && e.body.session_id === sid);
      const operationId = prompt("原补充指令operation ID（只观察，不重投）", candidates.at(-1)?.body.operation_id || "");
      if (!operationId) return;
      const scope = {project, session_id: sid, operation_id: operationId};
      const receipt = await api(`/api/coordinator/message?${new URLSearchParams(scope)}`);
      if (!receipt.operation || receipt.operation.operation_id !== operationId || receipt.operation.project !== project
          || receipt.operation.session_id !== sid) throw new Error("没有可归属原回执；未知不代表未发送");
      const result = await post("/api/coordinator/message/observe", scope);
      if (result.operation?.operation_id !== operationId || result.operation.project !== project
          || result.operation.session_id !== sid) throw new Error("观察回执身份不符，保留原意图");
      if (result.finished) {
        const entry = entries.find(e => e.body.operation_id === operationId);
        if (entry) { entry.state = "finished"; save(entries); }
      }
      toast(result.finished ? "原回合结束已核验；显式暂停/provider门不变" : `保持屏障：${result.reason || "尚无完整结束证据"}`);
      return;
    }
    if (entries.some(e => e.body.project === project && e.body.session_id === sid && !["finished", "refused"].includes(e.state))) {
      throw new Error("已有未决原指令；先观察结束，不换ID重复提交");
    }
    const control = await api(`/api/coordinator/control?${new URLSearchParams({project, session_id: sid})}`);
    if (control.read_only || control.runtime?.delivery_policy_guard_valid !== true || control.lease?.holder_session !== sid
        || !Number.isSafeInteger(control.session_control?.revision)) throw new Error("缺少当前控制/资源守卫，未发送");
    const text = prompt("补充指令（本指定入口默认按用户处理；不解除显式暂停）");
    if (!text) return;
    const confirm = prompt(`逐字确认项目 ${project}`);
    if (confirm !== project) throw new Error("确认不匹配，未发送");
    if (entries.length >= 128) {
      const completed = entries.findIndex(e => ["finished", "refused"].includes(e.state));
      if (completed < 0) throw new Error("未决意图已达上限，先对账；不丢原意图");
      entries.splice(completed, 1);
    }
    const body = {project, session_id: sid, operation_id: crypto.randomUUID(), text,
      reason: "web:explicit-user-message", expected_revision: control.session_control.revision,
      expected_epoch: control.lease.epoch, confirm};
    sending = {body, state: "possible"};
    entries.push(sending); save(entries); // 存储失败时零POST；未知不靠刷新/重启重发。
    const result = await post("/api/coordinator/message", body);
    if (result.operation?.operation_id !== body.operation_id || result.operation.project !== project || result.operation.session_id !== sid
        || typeof result.operation.message_id !== "string" || !["admitted", "finished"].includes(result.operation.state)) throw new Error("受理未知，保留原ID，只观察，不重投");
    sending.state = result.operation.state; save(entries);
    toast(`已受理 ${result.operation.message_id}；受理≠执行结束，自动待办先等本回合结束`);
  } catch (error) {
    if (sending && error.status >= 400 && error.status < 500 && error.code !== "BAD_RESPONSE") {
      sending.state = "refused";
      try {save(entries);} catch { /* 无法保存仍保留possible，不猜未执行 */ }
    }
    toast(error.message || "结果未知，保留原意图，不重投");
  }
  finally { state.busyActions.delete(key); publish(); }
}

// 验收表单：state.verifyForms 是 Map<routeKey, {attemptId, pass}>，key 形如 task:<id> / attempt:<id>。
// 仅负责按状态渲染表单；提交/关闭由 app 唯一委托（submit form[data-form=verify] / data-act=closeverify）。
// - 输入值来自 state.verifyDrafts（data.js/actions.js 的输入委托同步登记），value 绑定仅用于
//   “刷新/换节点后恢复草稿”：正常输入不触发 publish，焦点/caret 仍由浏览器管理，不被重绘打乱。
// - 同 route 换 attempt/pass 时用 keyed 强制重建表单，防止上一对象输入残留给新对象。
// - 提交期间（busyActions 含 verify:<attemptId>）禁用提交与输入，关闭按钮保持可用。
import {html, nothing} from "lit";
import {keyed} from "lit/directives/keyed.js";
import {state, ro} from "../state.js";
import {busy, verifySubmissionNote} from "../ui.js";

export function renderVerifyForm(key) {
  const entry = state.verifyForms.get(key);
  if (!entry) return nothing;
  const attemptId = entry.attemptId;
  const pass = Boolean(entry.pass);
  const locked = ro() || busy(`verify:${attemptId}`);
  const draft = state.verifyDrafts.get(key) || {};
  const v = name => draft[name] ?? "";
  return keyed(`${attemptId}:${pass ? 1 : 0}`, html`<form data-form="verify" data-key=${key} data-id=${attemptId} data-pass=${pass ? 1 : 0} class="card">
    <legend>验收 ${pass ? "pass" : "fail"}：<code class="mono">${attemptId}</code></legend>
    <label>证据（分号分隔，可留空）：<input type="text" name="evidence" .value=${v("evidence")} ?disabled=${locked}></label>
    <label>备注（可选）：<input type="text" name="notes" .value=${v("notes")} ?disabled=${locked}></label>
    <label>${pass ? "备注原因（可选；标准验收，不启用 force）：" : "失败原因（必填）："}<input type="text" name="reason" .value=${v("reason")} ?required=${!pass} ?disabled=${locked}></label>
    <div class="note">${verifySubmissionNote}</div>
    <label><input type="checkbox" name="run_checks" ?disabled=${locked}> 先运行 --check 验收命令</label>
    <label>确认：输入尝试 ID <input type="text" name="confirm" placeholder=${attemptId} .value=${v("confirm")} required ?disabled=${locked}></label>
    <div class="op-panel"><button class="primary" type="submit" ?disabled=${locked}>提交验收 ${pass ? "pass" : "fail"}</button><button type="button" data-act="closeverify" data-key=${key}>取消</button></div>
  </form>`);
}
// 返工表单：工作模式只影响 prompt；确认逐字回填任务 ID；提交由 app 唯一委托。
export function renderReworkForm(taskId) {
  const locked = ro();
  return html`<form data-form="rework" data-id=${taskId} class="card">
    <legend>返工/继续：<code class="mono">${taskId}</code></legend>
    <label>反馈（必填）<textarea name="feedback" required ?disabled=${locked}></textarea></label>
    <label>工作模式<select name="work_mode" ?disabled=${locked}>
      <option value="latency-first">latency-first / fast（时效优先，默认）</option>
      <option value="budget-first">budget-first / budget（预算优先）</option>
      <option value="elastic">elastic（弹性加速）</option>
    </select></label>
    <div class="note">工作模式只写入提示词，不改变模型/资源/费用授权；高风险任务仍按验收标准执行。</div>
    <label>确认：输入任务 ID <input type="text" name="confirm" placeholder=${taskId} required ?disabled=${locked}></label>
    <div class="op-panel"><button class="primary" type="submit" ?disabled=${locked}>提交返工</button></div>
  </form>`;
}

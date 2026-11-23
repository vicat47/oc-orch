// 写操作与全局交互委托。
// 原则：客户端先做 read_only 检查与旧确认/提示；POST 沿用 X-OC-Orch-Web + Origin
// 后端守卫；未知请求不自动重投、不显示成功；busyActions 去重（写动作未完成，
// 重复 click 不发第二次请求）。
import {api, post} from "./api.js";
import {state, publish, ro} from "./state.js";
import {copyText, toast, markDirty} from "./ui.js";
import {parseHash, routeKey} from "./router.js";
import {loadMoreCoord, loadMoreUnbound, manualRefresh, refreshLight} from "./data.js";
import {submitPolicy, recheckPolicy, rememberPolicyInput} from "./policy-actions.js";
import {conversationAction} from "./operator-actions.js";
import {rememberPrototypeInput, submitPrototype, recheckPrototype} from "./prototype-actions.js";

const RO_TOAST = "read-only：此操作被后端拒绝（当前为只读快照）";

// 写动作包装：ro 检查 + busy 去重 + 失败不重投。
async function guard(key, fn) {
  if (ro()) { toast(RO_TOAST); return; }
  if (state.busyActions.has(key)) return;
  state.busyActions.add(key);
  publish();
  try {
    await fn();
  } catch (error) {
    toast(error.message || "操作失败（未重试，请手动重试）");
  } finally {
    state.busyActions.delete(key);
    publish();
  }
}

function refresh() {
  // 写成功后静默刷新事实；不抛错掩盖成功动作。
  try { refreshLight(); } catch (_) { /* ignore */ }
}

function taskPause(id) {
  guard(`taskpause:${id}`, async () => {
    if (!confirm(`暂停任务 ${id}？`)) return;
    await post(`/api/task/${encodeURIComponent(id)}/pause`, {reason: "web-pause"});
    toast("已暂停");
    await refreshLight();
  });
}

function taskResume(id) {
  guard(`taskresume:${id}`, async () => {
    await post(`/api/task/${encodeURIComponent(id)}/resume`, {});
    toast("已恢复");
    await refreshLight();
  });
}

function taskCancel(id) {
  guard(`taskcancel:${id}`, async () => {
    const reason = prompt("取消原因", "web-cancel");
    if (reason === null) return;
    const c = prompt(`输入任务 ID 确认取消：${id}`);
    if (c !== id) { alert("确认不匹配"); return; }
    const interrupt = confirm("同时尝试 interrupt 执行 session？");
    await post(`/api/task/${encodeURIComponent(id)}/cancel`, {reason, confirm: c, interrupt});
    toast("已取消");
    await refreshLight();
  });
}

function collect(id) {
  guard(`collect:${id}`, async () => {
    if (!confirm(`collect ${id}？`)) return;
    await post(`/api/attempt/${encodeURIComponent(id)}/collect`, {});
    toast("已收集");
    await refreshLight();
  });
}

let formToken = 0;
function openVerify(el) {
  if (ro()) { toast(RO_TOAST); return; }
  const attemptId = el.dataset.id;
  if (!attemptId) return;
  const pass = el.dataset.pass === "1";
  // 视图合同：state.verifyForms 的 key 即 routeKey（task:<id> / attempt:<id>），
  // forms.js 以该 key 查表渲染表单；跨对象表单由 app 的 keyed 路由隔离，不会互写。
  // entry 带唯一 token：提交回执按 token 比对身份，同 attempt 关开/切 pass 不误删新表单。
  const key = routeKey(parseHash());
  state.verifyDrafts.delete(key); // 新表单清理旧草稿，避免上一对象输入残留
  const entry = {attemptId, pass, token: ++formToken};
  state.verifyForms.set(key, entry);
  publish();
  requestAnimationFrame(() => {
    // 首帧较晚时用户可能已在证据框输入；旧回调不得抢走焦点或跨表单定位。
    if (state.verifyForms.get(key) !== entry || routeKey(parseHash()) !== key) return;
    const form = document.querySelector('form[data-form="verify"]');
    if (form?.dataset.key !== key || document.activeElement?.matches?.("input,textarea,select,[contenteditable]")) return;
    form.querySelector('[name="confirm"]')?.focus({preventScroll: true});
  });
}

function closeVerify(el) {
  const key = el?.dataset?.key || routeKey(parseHash());
  if (key && state.verifyForms.delete(key)) {
    state.verifyDrafts.delete(key);
    markDirty();
    publish();
    return;
  }
  // 兼容：按钮不带 data-key 且路由不匹配时按 attemptId 匹配。
  const aid = el?.dataset?.id;
  if (aid) {
    let hit = false;
    for (const [k, v] of state.verifyForms) {
      if (v.attemptId === aid) { state.verifyForms.delete(k); hit = true; }
    }
    if (hit) { markDirty(); publish(); }
  }
}

function unlock(sid, project, allowProvider) {
  guard(`unlock:${sid}`, async () => {
    if (!sid) return;
    const label = allowProvider ? "恢复调度" : "解锁 manual";
    const reason = prompt(`解锁原因（${label}）`, allowProvider ? "web-provider-recovery" : "web-manual-unlock");
    if (reason === null) return;
    const c = prompt(`输入 UNLOCK 确认（${label} ${sid}，项目 ${project || "?"}）`);
    if (c !== "UNLOCK") { alert("确认不匹配"); return; }
    const payload = { project, session: sid, reason, confirm: "UNLOCK" };
    if (allowProvider) payload.allow_provider = true;
    const j = await post("/api/coordinator/unlock", payload);
    const r = j?.result || {};
    toast(r.cleared ? `${label} 完成 ${sid}；提前 ${r.resurfaced || 0} 条待投递` : "没有活动闸门");
    await refreshLight();
  });
}

function ackNote(id) {
  guard(`acknote:${id}`, async () => {
    if (!id || !confirm(`ACK 通知 ${id}？`)) return;
    await post("/api/notify/ack", {ids: [id]});
    toast("已 ACK");
    await refreshLight();
  });
}

function ackChecked(scopeSel, url) {
  if (ro()) { toast(RO_TOAST); return; }
  // 安全规格：只 ACK 当前活动视图可见表格中勾选的具体集合，由 scopeSel 限定。
  const ids = [...document.querySelectorAll(scopeSel)].map(c => c.dataset.id).filter(Boolean);
  if (!ids.length) { alert("未勾选任何项"); return; }
  // 同端点统一锁，避免并发批量 ACK 交错。
  const lock = `ackchecked:${url}`;
  if (state.busyActions.has(lock)) return;
  // 集合全量列出（快照通知/告警 ≤50 条），不缩略为“前 8 条”。
  const list = ids.join("\n");
  const kind = url.endsWith("/notify/ack") ? "通知" : "告警";
  if (!confirm(`批量 ACK ${ids.length} 条${kind}？\n集合（共 ${ids.length} 条）：\n${list}`)) return;
  if (state.busyActions.has(lock)) return;
  state.busyActions.add(lock);
  publish();
  (async () => {
    try {
      await post(url, {ids});
      toast(`已 ACK ${ids.length} 条`);
      await refreshLight();
    } catch (error) {
      toast(error.message || "操作失败");
    } finally {
      state.busyActions.delete(lock);
      publish();
    }
  })();
}

function supStart() {
  guard("supstart:supervisor", async () => {
    const c = prompt("输入 START 确认启动 Supervisor");
    if (c !== "START") { alert("确认不匹配"); return; }
    await post("/api/supervisor/start", {confirm: "START"});
    toast("已启动");
    await refreshLight();
  });
}

function supStop() {
  guard("supstop:supervisor", async () => {
    if (!confirm("停止 Supervisor？台账保留，worker session 不受影响。")) return;
    const c = prompt("输入 STOP 确认");
    if (c !== "STOP") { alert("确认不匹配"); return; }
    await post("/api/supervisor/stop", {confirm: "STOP"});
    toast("已停止");
    await refreshLight();
  });
}

function handleManaged(el) {
  if (ro()) { toast(RO_TOAST); return; }
  const act = el.dataset.managed;
  const id = el.dataset.id;
  if (!act || !id) return;
  const key = `managed:${act}:${id}`;
  if (state.busyActions.has(key)) return;
  const reason = prompt(`${act} 原因`, "web-" + act);
  if (reason === null) return;
  const confirmText = prompt(`输入输入 ID 确认 ${act}：${id}`);
  if (confirmText !== id) { alert("确认不匹配"); return; }
  const body = {
    owner: "web",
    confirm: id,
    expected_revision: parseInt(el.dataset.rev || "1", 10) || 1,
    reason,
  };
  state.busyActions.add(key);
  publish();
  (async () => {
    try {
      if (act === "ack") body.value = confirm("业务 ACK 确认为成功？");
      await post(`/api/input/${encodeURIComponent(id)}/${act}`, body);
      toast(`${act} 已提交`);
      await refreshLight();
    } catch (error) {
      toast(error.message || "操作失败");
    } finally {
      state.busyActions.delete(key);
      publish();
    }
  })();
}

function coordMore(sid) { loadMoreCoord(sid); }
function unboundMore() { loadMoreUnbound(); }

// 项目暂停/继续：只读当前健康投影里的真实 project_revision/epoch 作 CAS 入参，
// confirm 必须逐字等于项目路径；未取得控制版本、确认不匹配或后端拒绝（400）
// 一律不显示成功，由 guard 的 toast 如实提示失败。
function projectControl(act, el) {
  const project = el?.dataset?.project;
  const sid = el?.dataset?.sid;
  if (!project || !sid) return;
  guard(`project${act}:${sid}:${project}`, async () => {
    const label = act === "pause" ? "暂停" : "继续";
    const c = prompt(`输入项目路径确认${label}：${project}`);
    if (c !== project) { alert("确认不匹配"); return; }
    const health = state.healthBySid[sid]?.body;
    const task = (health?.tasks || []).find(t => t.project === project && t.control);
    const control = task?.control;
    if (!control || typeof control.project_revision !== "number"
        || typeof control.epoch !== "number") {
      alert("缺少当前项目控制版本（project_revision/epoch），操作拒绝；请刷新后重试");
      return;
    }
    const operationId = `web-${act}-${Date.now().toString(36)}`;
    const j = await post(`/api/coordinator/${act}`, {
      project,
      session_id: sid,
      operation_id: operationId,
      reason: `web 显式${label}`,
      expected_revision: control.project_revision,
      expected_epoch: control.epoch,
      confirm: project,
    });
    const r = (j?.result || {});
    toast(r.applied === false
      ? `${label}已处理但本次未变更（以当前状态为准）`
      : `${label}已应用：project=${project} revision=${r.revision ?? "?"}`);
    await refreshLight();
  });
}

function manual() {
  if (state.busyActions.has("manualrefresh")) return;
  state.busyActions.add("manualrefresh");
  publish();
  manualRefresh().finally(() => {
    state.busyActions.delete("manualrefresh");
    publish();
  });
}

/* 回合 hold：只读候选 + 两个显式恢复入口。两者都先取当前控制版本做 CAS；
   不在浏览器推断 hold/回合，也不把“登记”当作发送/验收。 */
async function controlVersion(project, sid) {
  const control = await api(`/api/coordinator/control?${new URLSearchParams({project, session_id: sid})}`);
  if (control.read_only || control.lease?.holder_session !== sid
      || !Number.isSafeInteger(control.session_control?.revision)
      || !Number.isSafeInteger(control.lease?.epoch)) {
    throw new Error("缺少当前控制/租约版本，操作拒绝；请刷新后重试");
  }
  return control;
}

function checkTurnCandidate(el) {
  const project = el?.dataset?.project;
  const sid = el?.dataset?.sid;
  if (!project || !sid) return;
  guard(`checkturn:${sid}`, async () => {
    const candidate = await api(`/api/coordinator/turn-candidate?${new URLSearchParams({project, session_id: sid})}`);
    state.turnCandidates[sid] = candidate;
    toast(candidate.eligible
      ? `可登记用户输入 ${candidate.message_id}`
      : `不可登记：${candidate.reason || "unknown"}`);
    publish();
  });
}

function registerTurn(el) {
  const project = el?.dataset?.project;
  const sid = el?.dataset?.sid;
  const candidate = state.turnCandidates[sid];
  if (!project || !sid || !candidate?.eligible || !candidate.message_id) return;
  guard(`registerturn:${sid}`, async () => {
    const control = await controlVersion(project, sid);
    if (control.session_control.hold == null) throw new Error("当前已无回合 hold，未登记");
    if (control.session_control.barrier != null) throw new Error("已有用户回合屏障，未登记");
    const confirmText = prompt(`登记用户输入 ${candidate.message_id} 并恢复调谐？输入项目路径确认：${project}`);
    if (confirmText !== project) { toast("确认不匹配，未登记"); return; }
    const body = {
      project, session_id: sid,
      operation_id: `web-register-turn-${Date.now().toString(36)}`,
      reason: "web:operator-registered-completed-turn",
      message_id: candidate.message_id,
      expected_revision: control.session_control.revision,
      expected_epoch: control.lease.epoch,
      confirm: project,
    };
    const response = await post("/api/coordinator/register-turn", body);
    const result = response.result || {};
    if (response.ok !== true || result.executed !== true) {
      toast(`未登记：${response.refused || result.refused || "unknown"}（未自动重试）`);
      await refreshLight();
      return;
    }
    delete state.turnCandidates[sid];
    toast(`已登记并结束用户回合：${result.reason || "ok"}；仍需其他门禁通过才自动唤醒`);
    await refreshLight();
  });
}

function releaseHold(el) {
  const project = el?.dataset?.project;
  const sid = el?.dataset?.sid;
  if (!project || !sid) return;
  guard(`releasehold:${sid}`, async () => {
    const control = await controlVersion(project, sid);
    if (control.session_control.hold == null) { toast("当前已无回合 hold"); return; }
    if (control.session_control.barrier != null) {
      toast("仍有用户回合屏障；先观察/结束该回合，不强制解除");
      return;
    }
    const reason = prompt("解除原因", "web:operator-forced-resume");
    if (!reason) return;
    const confirmText = prompt(`输入项目路径确认强制解除：${project}`);
    if (confirmText !== project) { toast("确认不匹配，未解除"); return; }
    const token = prompt(`输入 RESUME 确认解除 ${sid} 的回合 hold`);
    if (token !== "RESUME") { toast("确认词不匹配，未解除"); return; }
    const body = {
      project, session_id: sid,
      operation_id: `web-release-hold-${Date.now().toString(36)}`,
      reason,
      expected_revision: control.session_control.revision,
      expected_epoch: control.lease.epoch,
      confirm: project,
    };
    const response = await post("/api/coordinator/release-hold", body);
    const result = response.result || {};
    if (response.ok !== true || result.executed !== true) {
      toast(`未解除：${response.refused || result.refused || "unknown"}（未自动重试）`);
      await refreshLight();
      return;
    }
    delete state.turnCandidates[sid];
    toast(result.applied === false
      ? "已无 hold（幂等），未改写控制版本"
      : `已解除 hold；提前通知 ${response.resurfaced ?? 0} 条，仍需其他门禁通过`);
    await refreshLight();
  });
}

export function handleAct(el) {
  const act = el.dataset.act;
  switch (act) {
    case "taskpause": taskPause(el.dataset.id); break;
    case "taskresume": taskResume(el.dataset.id); break;
    case "taskcancel": taskCancel(el.dataset.id); break;
    case "collect": collect(el.dataset.id); break;
    case "verify": openVerify(el); break;
    case "closeverify": closeVerify(el); break;
    case "unlock": unlock(el.dataset.sid, el.dataset.project, el.dataset.allowProvider === "1" || el.dataset.allowProvider === "true"); break;
    case "acknote": ackNote(el.dataset.id); break;
    case "ackchecked": ackChecked("#notifTable .noteck:checked", "/api/notify/ack"); break;
    case "ackalerts": ackChecked("#alertTable .alck:checked", "/api/alerts/ack"); break;
    case "supstart": supStart(); break;
    case "supstop": supStop(); break;
    case "coordmore": coordMore(el.dataset.sid); break;
    case "unboundmore": unboundMore(); break;
    case "projpause": projectControl("pause", el); break;
    case "projresume": projectControl("resume", el); break;
    case "usersend": conversationAction(el); break;
    case "userobserve": conversationAction(el, true); break;
    case "checkturn": checkTurnCandidate(el); break;
    case "registerturn": registerTurn(el); break;
    case "releasehold": releaseHold(el); break;
    case "prototypefeature": state.prototypeSelected.set(el.dataset.task, el.dataset.feature); publish(); break;
    case "prototyperecheck": recheckPrototype(el.dataset.task); break;
    case "manualrefresh": manual(); break;
    case "policyrecheck": recheckPolicy(el.dataset.scope, el.dataset.target); break;
    default: /* 未知 action 忽略，不冒充已知语义 */ break;
  }
}

function handleTab(el) {
  const value = el.dataset.tab;
  if (!value) return;
  const host = el.dataset.tabHost || "coordinator";
  if (state.tab[host] === value) return;
  state.tab[host] = value;
  publish();
}

// 验收表单提交：force=false 保持旧后端行为；客户端超时/失败不自动重投。
// 回执删除按提交时捕获的 entry token 比对身份：同 attempt 关闭再打开/切 pass
// 产生的新表单（新 token）不会被旧回执误删（“旧动作回执不关闭新表单”）。
// 取值以 state.verifyDrafts（输入事件同步登记）为准、DOM 为兜底：任一后台刷新
// 重建输入节点都不会丢掉用户已输入的内容（证据/备注/原因/确认），提交不被
// 易失 DOM 状态左右。
export async function submitVerify(form) {
  if (ro()) { toast(RO_TOAST); return; }
  const attemptId = form.dataset.id;
  const pass = form.dataset.pass === "1";
  const key = form.dataset.key || routeKey(parseHash());
  const entryAtSubmit = state.verifyForms.get(key);
  if (!entryAtSubmit) return;
  const drafts = state.verifyDrafts.get(key) || {};
  const valueOf = name => {
    const draftValue = drafts[name];
    if (typeof draftValue === "string") return draftValue;
    return form.elements?.[name]?.value ?? "";
  };
  const reason = valueOf("reason").trim();
  if (!pass && !reason) { alert("验收失败需要原因"); return; }
  if (valueOf("confirm") !== attemptId) { alert("确认不匹配"); return; }
  const bkey = `verify:${attemptId}`;
  if (state.busyActions.has(bkey)) return; // busy 去重：重复提交不发第二次请求
  state.busyActions.add(bkey);
  publish();
  try {
    await post(`/api/attempt/${encodeURIComponent(attemptId)}/verify`, {
      pass,
      confirm: attemptId,
      evidence: valueOf("evidence").split(";").map(s => s.trim()).filter(Boolean),
      notes: valueOf("notes"),
      reason,
      force: false, // 沿用旧后端：不宣称 force/人工例外
      run_checks: !!form.elements?.run_checks?.checked,
    });
    toast("验收已提交");
    const current = state.verifyForms.get(key);
    if (current && current.token === entryAtSubmit.token) {
      state.verifyForms.delete(key);
      state.verifyDrafts.delete(key);
    }
    publish();
    refresh();
  } catch (error) {
    // 不自动重投、不显示成功；若表单已被用户替换，保留新表单。
    toast(error.message || "验收提交失败");
    markDirty();
  } finally {
    state.busyActions.delete(bkey);
    publish();
  }
}

// 发送受管输入（execute / park / record），字段沿旧页面。
// text 保留原始内容（仅用 trim 做必填校验），不改写用户草稿；成功提示后仅当
// 原表单仍连接且输入值与提交时一致才 reset，避免清掉请求发出后继续输入的新草稿。
function sendInput(form) {
  if (ro()) { toast(RO_TOAST); return; }
  const fd = new FormData(form);
  const body = {};
  const submitted = {};
  for (const k of ["session", "intent", "text", "owner", "request_id", "project", "task", "attempt", "target_session"]) {
    const v = String(fd.get(k) || "");
    submitted[k] = v;
    if (k === "text") { body.text = v; }                 // 原样发送
    else if (v.trim()) { body[k] = v.trim(); }
  }
  if (!body.session || !body.intent || !body.text?.trim()) { alert("session / intent / text 必填"); return; }
  const key = `send:${body.session}:${body.request_id || body.intent}`;
  if (state.busyActions.has(key)) return;
  state.busyActions.add(key);
  publish();
  (async () => {
    try {
      await post("/api/input/send", body);
      toast("已发送（受理 ≠ 投递 ≠ 执行 ≠ 业务 ACK）");
      // 快照守卫：仅当原表单仍在文档且各字段值未变才 reset。
      if (form.isConnected) {
        const cur = new FormData(form);
        let unchanged = true;
        for (const [k, v] of Object.entries(submitted)) {
          if (String(cur.get(k) || "") !== v) { unchanged = false; break; }
        }
        if (unchanged) form.reset();
      }
      await refreshLight();
    } catch (error) {
      toast(error.message || "发送失败");
    } finally {
      state.busyActions.delete(key);
      publish();
    }
  })();
}

/* ============ 全局委托入口（app 根节点各绑定一次） ============ */
export function handleClick(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;
  const copyEl = target.closest?.("[data-copy]");
  if (copyEl) { copyText(copyEl.dataset.copy || ""); return; }
  const tabEl = target.closest?.("[data-tab]");
  if (tabEl) { handleTab(tabEl); return; }
  const managedEl = target.closest?.("[data-managed]");
  if (managedEl) { handleManaged(managedEl); return; }
  const actEl = target.closest?.("[data-act]");
  if (actEl) { handleAct(actEl); }
}

async function submitRework(form) {
  if (ro()) { toast(RO_TOAST); return; }
  const id = form.dataset.id;
  const fd = new FormData(form);
  const feedback = String(fd.get("feedback") || "").trim();
  const confirm = String(fd.get("confirm") || "");
  const work_mode = String(fd.get("work_mode") || "latency-first");
  if (!id || !feedback) { toast("返工反馈不能为空"); return; }
  if (confirm !== id) { toast("确认不匹配，未返工"); return; }
  guard(`rework:${id}`, async () => {
    const response = await post(`/api/task/${encodeURIComponent(id)}/rework`,
                                {feedback, work_mode, confirm});
    const result = response.result || {};
    toast(`已创建返工尝试 ${result.attempt_id || "?"} g${result.generation ?? "?"}，模式 ${work_mode}`);
    await refreshLight();
  });
}

export function handleSubmit(event) {
  const form = event.target;
  if (!form || form.tagName !== "FORM") return;
  const kind = form.dataset.form;
  if (kind === "verify") {
    event.preventDefault();
    submitVerify(form);
  } else if (kind === "rework") {
    event.preventDefault();
    submitRework(form);
  } else if (kind === "send") {
    event.preventDefault();
    sendInput(form);
  } else if (kind === "delivery-policy") {
    event.preventDefault();
    submitPolicy(form);
  } else if (kind === "prototype") {
    event.preventDefault();
    submitPrototype(form);
  }
}

// taskSearch 同步更新再 publish：防止后台刷新用旧 state.search 覆盖输入框值。
export function handleInput(event) {
  const target = event.target;
  rememberPolicyInput(target);
  rememberPrototypeInput(target);
  if (target?.id === "taskSearch") {
    state.search = target.value;
    publish();
  }
  const verifyForm = target?.closest?.('form[data-form="verify"]');
  if (verifyForm?.dataset?.key && target?.name && typeof target.value === "string") {
    // 验收草稿：不 publish（保持输入不重绘/光标稳定），仅登记供提交与换节点恢复。
    const key = verifyForm.dataset.key;
    const drafts = state.verifyDrafts.get(key) || {};
    drafts[target.name] = target.value;
    state.verifyDrafts.set(key, drafts);
  }
  if (target?.closest?.("form[data-form]")) state.dirtyForm = true;
}

export function handleChange(event) {
  const target = event.target;
  rememberPolicyInput(target);
  rememberPrototypeInput(target);
  if (target?.id === "auto") {
    state.auto = target.checked;
    publish();
    return;
  }
  if (target?.matches?.('form[data-form]')) state.dirtyForm = true;
}

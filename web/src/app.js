import {LitElement, html, nothing} from "lit";
import {cache} from "lit/directives/cache.js";
import {keyed} from "lit/directives/keyed.js";
import * as overviewView from "./views/overview.js";
import * as coordinatorView from "./views/coordinator.js";
import * as taskView from "./views/task.js";
import * as attemptView from "./views/attempt.js";
import * as tasksView from "./views/tasks.js";
import * as diagnosticsView from "./views/diagnostics.js";
import {state, subscribe, ro} from "./state.js";
import {parseHash, routeKey, needDetail, navigate, restoreNavigation} from "./router.js";
import {fmtAge, processStatus, connectionStatus} from "./ui.js";
import * as data from "./data.js";
import * as actions from "./actions.js";

const VIEW_LABEL = {
  overview: "调度总览",
  coordinator: "调度者工作台",
  task: "任务详情",
  attempt: "尝试详情",
  tasks: "全局任务",
  diagnostics: "系统诊断",
};

export class OcOrchApp extends LitElement {
  // light DOM：共享 /assets/app.css，不使用 shadow，导航/健康/自动刷新由 Lit 管理。
  createRenderRoot() { return this; }

  connectedCallback() {
    super.connectedCallback();
    // 生命周期 session：disconnect 时置灰，连接断开/重启后旧回调（订阅/哈希/续建 ticker）全部失效，
    // 重新 append 会以新 session 重新走完整初始化（连接/断开对称，不残留无订阅的空壳）。
    const session = {alive: true};
    this._lifecycle = session;
    // 唯一事件委托：click/submit/input/change 在根节点各绑定一次，刷新不重复 addEventListener。
    this._onClick = e => actions.handleClick(e);
    this._onSubmit = e => actions.handleSubmit(e);
    this._onInput = e => actions.handleInput(e);
    this._onChange = e => actions.handleChange(e);
    this.addEventListener("click", this._onClick);
    this.addEventListener("submit", this._onSubmit);
    this.addEventListener("input", this._onInput);
    this.addEventListener("change", this._onChange);
    this._unsub = subscribe(() => { if (this.isConnected && this._lifecycle === session) this.requestUpdate(); });
    this._onHash = () => { if (this._lifecycle === session) this.handleRoute(); };
    window.addEventListener("hashchange", this._onHash);
    this.boot(session);
  }

  disconnectedCallback() {
    super.disconnectedCallback();
    const session = this._lifecycle;
    this._lifecycle = null;
    if (session) session.alive = false; // 使进行中的 boot/订阅/哈希回调立即失效
    clearInterval(this._ticker);
    this._ticker = null;
    if (this._onHash) window.removeEventListener("hashchange", this._onHash);
    this._onHash = null;
    if (this._onClick) {
      this.removeEventListener("click", this._onClick);
      this.removeEventListener("submit", this._onSubmit);
      this.removeEventListener("input", this._onInput);
      this.removeEventListener("change", this._onChange);
    }
    this._onClick = this._onSubmit = this._onInput = this._onChange = null;
    this._unsub?.();
    this._unsub = null;
  }

  async boot(session) {
    if (!session.alive || this._lifecycle !== session) return;
    state.ready = false; // 首屏 deep-link：详情请求尚未发出时显示加载而非“加载失败”
    await Promise.all([data.refreshStatus(), data.refreshCoordinators(), data.refreshExtras()]);
    // 断开/重启保护：若连接期间被移除，不再 handleRoute、不创建泄漏的定时器。
    if (!session.alive || this._lifecycle !== session) return;
    state.ready = true;
    this.handleRoute();
    this._ticker = setInterval(() => this.tick(), 5000);
  }

  tick() {
    if (!state.auto || document.hidden || !this._lifecycle) return;
    data.refreshLight();
  }

  handleRoute() {
    const route = parseHash();
    const key = routeKey(route);
    const changed = key !== state.currentKey;
    navigate(); // 保存滚动、代次+1、更新 currentKey、publish
    if (changed) {
      if (needDetail(route)) {
        const entry = state.details.get(key);
        if (entry?.body && !state.pendingRoute.has(key)) {
          restoreNavigation(key);   // 已有缓存：立即恢复一次滚动/focus
        } else {
          state.pendingFocus = key; // 无缓存：等 loadDetail 完成后再恢复一次
        }
      } else {
        restoreNavigation(key);     // 同步视图：直接恢复
      }
    }
    if (needDetail(route)) {
      data.loadDetail(route).catch(() => {});
    }
    data.refreshLight();
  }

  skipToMain(event) {
    event.preventDefault();
    document.getElementById("main")?.focus();
  }

  render() {
    const route = parseHash();
    return html`
      <a class="skip" href="#main" @click=${this.skipToMain}>跳到主内容</a>
      <header>${this.headerTemplate(route)}</header>
      <main id="main" tabindex="-1">
        ${this.routeView(route)}
      </main>
      <div id="toast" class="toast" role="status" aria-live="polite"></div>
    `;
  }

  // 视图级 DOM 缓存：cache 以外层每类视图的稳定模板字面量为键（不同 view 模板 literal），
  // 页面切换走 cache 冻结/恢复，同视图后台刷新同模板原地更新（不替换 DOM、不丢焦点/滚动）。
  // keyed 在 cache 内部以 routeKey 为键做跨对象隔离：同视图不同对象 A→B 重建子树，
  // 避免对象 A 的表单/展开状态落到对象 B。
  routeView(route) {
    const key = routeKey(route);
    switch (route.view) {
      case "overview":
        return cache(html`<div id="overviewRoot" aria-live="polite">${keyed(key, this.overviewBody(route))}</div>`);
      case "coordinator":
        return cache(html`<div id="coordRoot" aria-live="polite">${keyed(key, this.coordinatorBody(route))}</div>`);
      case "task":
        return cache(html`<div id="taskRoot" aria-live="polite">${keyed(key, this.taskBody(route))}</div>`);
      case "attempt":
        return cache(html`<div id="attemptRoot" aria-live="polite">${keyed(key, this.attemptBody(route))}</div>`);
      case "tasks":
        return cache(html`<div id="tasksRoot" aria-live="polite">${keyed(key, this.tasksBody(route))}</div>`);
      case "diagnostics":
        return cache(html`<section id="view-diagnostics" aria-label="系统诊断">${keyed(key, this.diagnosticsBody(route))}</section>`);
      default:
        return html`<div class="attn rank-mid"><span>未知视图：${route.view}</span></div>`;
    }
  }

  overviewBody(route) {
    if (!state.snap && !state.statusErr) return loadingPanel(VIEW_LABEL.overview);
    return html`${errorBanner(state.statusErr || state.coordErr || state.extraErr)}${overviewView.renderOverview()}`;
  }

  coordinatorBody(route) {
    return this.detailBody(route, body => coordinatorView.renderCoordinator(route.sid, body));
  }

  taskBody(route) {
    return this.detailBody(route, body => taskView.renderTask(route.id, body));
  }

  attemptBody(route) {
    return this.detailBody(route, body => attemptView.renderAttempt(route.id, body));
  }

  tasksBody(route) {
    if (route.unbound) return this.detailBody(route, () => tasksView.renderUnboundTasks());
    if (!state.snap && !state.statusErr) return loadingPanel(VIEW_LABEL.tasks);
    return html`${errorBanner(state.statusErr)}${tasksView.renderTasksView()}`;
  }

  diagnosticsBody(route) {
    if (!state.snap && !state.statusErr) return loadingPanel(VIEW_LABEL.diagnostics);
    return html`${errorBanner(state.extraErr || state.statusErr || (route.section === "responsibilities" && state.coordErr))}${diagnosticsView.renderDiagnostics(route.section)}`;
  }

  detailBody(route, renderFn) {
    const key = routeKey(route);
    const entry = state.details.get(key);
    const pending = state.pendingRoute.has(key);
    // 初始化期（首屏 deep-link：详情请求尚未发出）显示加载，而不是“加载失败”。
    if (!state.ready) return loadingPanel(VIEW_LABEL[route.view] || "详情");
    // 只初次路由且无缓存才显示加载；后台刷新保留旧 DOM、不先加载中。
    if (pending && !entry?.body) return loadingPanel(VIEW_LABEL[route.view] || "详情");
    // 真实 404：明确对象缺失，而不是继续显示可写的旧对象。
    if (entry?.notFound) return missingPanel(route);
    if (!entry?.body) {
      // 请求确已发出且失败（网络/超时）：与 404 分开，展示错误与重试。
      return loadFailedPanel(entry?.error);
    }
    const banner = entry?.error ? staleBanner(entry) : nothing;
    return html`${banner}${renderFn(entry.body)}`;
  }

  headerTemplate(route) {
    const navId = ["tasks", "task", "attempt"].includes(route.view) ? "tasks" : route.view === "diagnostics" ? "diagnostics" : "overview";
    return html`
      <h1><a class="brand" href="#/" aria-label="返回调度总览"><span class="brand-mark" aria-hidden="true"></span>oc-orch</a></h1>
      <nav id="nav" aria-label="主导航">
        <a href="#/" data-nav="overview" aria-current=${navId === "overview" ? "page" : nothing}>总览</a>
        <a href="#/tasks" data-nav="tasks" aria-current=${navId === "tasks" ? "page" : nothing}>全局任务</a>
        <a href="#/diagnostics" data-nav="diagnostics" aria-current=${route.view === "diagnostics" ? "page" : nothing}>系统诊断</a>
      </nav>
      <div class="header-tools">
      ${this.healthTemplate()}
      <label class="inline" title="5 秒自动刷新"><input type="checkbox" id="auto" ?checked=${state.auto}> 自动刷新</label>
      <button id="refreshBtn" data-act="manualrefresh" ?disabled=${state.refreshing || state.busyActions.has("manualrefresh")}>
        ${state.refreshing ? "刷新中…" : "刷新"}
      </button></div>
    `;
  }

  healthTemplate() {
    const s = state.snap?.supervisor || {};
    const svc = state.snap?.service || {};
    const process = processStatus(s, state.statusErr);
    const anyErr = state.statusErr || state.coordErr || state.extraErr || Object.keys(state.errors).length > 0;
    return html`<details id="health">
      <summary><span class=${`presence ${process.tone}`}>调度服务${process.text}</span>
        ${anyErr ? html`<span class="warn">部分数据过期</span>` : nothing}
        ${ro() ? html`<span class="badge">只读</span>` : nothing}</summary>
      <dl class="kv health-detail">
        <dt>状态更新</dt><dd>${state.statusAt ? fmtAge(Date.now() - state.statusAt) + "前" : "尚未读取"}</dd>
        <dt>会话更新</dt><dd>${state.coordAt ? fmtAge(Date.now() - state.coordAt) + "前" : "尚未读取"}</dd>
        <dt>进程 ID</dt><dd>${s.pid ?? "未知"}</dd>
        <dt>事件连接</dt><dd>${connectionStatus(s, state.statusErr)}</dd>
        ${svc.base ? html`<dt>服务地址</dt><dd>${svc.base} ${svc.version || ""}</dd>` : nothing}
      </dl></details>`;
  }
}

function loadingPanel(title) {
  return html`<h2>${title || "加载中…"}</h2><div class="muted" role="status">正在从服务端读取数据…</div>`;
}
function errorBanner(flag) {
  return flag ? html`<div class="attn rank-mid"><span class="warn">部分数据刷新失败，显示内容可能过期。</span> <button data-act="manualrefresh">重试</button></div>` : nothing;
}
function staleBanner(entry) {
  return html`<div class="attn rank-mid"><span class="warn">详情刷新失败，当前显示缓存的旧内容：${entry.error?.message || ""} <button data-act="manualrefresh">重试</button></span></div>`;
}
function loadFailedPanel(error) {
  return html`<h2>加载失败</h2>
    <div class="attn rank-high"><span>无法读取详情：${error?.message || "网络或服务错误"} <button data-act="manualrefresh">重试</button> <a href="#/">返回总览</a></span></div>`;
}
function missingPanel(route) {
  const text = route.view === "coordinator"
    ? `当前列表中找不到该会话：${route.sid}`
    : `找不到该记录：${route.id}`;
  return html`<h2>${VIEW_LABEL[route.view] || "详情"}</h2>
    <div class="attn rank-high"><span>${text}。<a href="#/">返回总览</a> · <a href="#/tasks">全局任务</a></span></div>`;
}

customElements.define("oc-orch-app", OcOrchApp);
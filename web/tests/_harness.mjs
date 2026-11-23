/**
 * oc-orch web 真实 DOM 测试基座（jsdom + 源码模块共享实例）。
 *
 * 设计：每个测试文件在一个 jsdom 窗口内运行；`boot()` 首次导入 web/src/app.js
 * （与 data/state/ui 等模块共享同一实例），之后每次测试 reset 状态并刷新路由。
 * 与前端 worker 的接口契约（父会话已确认 views 组/核心按此实现）：
 * - app.js 顶层 `customElements.define("oc-orch-app")`，light DOM 渲染。
 * - data.js 导出 `refreshLight`（单飞）等；state.js 导出 `state`；均由本基座
 *   直接 import，与页面运行在同一模块实例上（共享状态）。
 * - 事件经 data-act / data-copy / data-tab / data-managed / data-form 委托；
 *   confirm/prompt 走 window（测试可注入返回值并记录对话）；read_only 下写控件
 *   禁用或隐藏。
 * - 资源为本地同源（app.css / app.js），无 CDN。
 */
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { JSDOM } from "jsdom";
import { after } from "node:test";

const here = path.dirname(fileURLToPath(import.meta.url));
const SRC = path.resolve(here, "../src");
const ASSETS = path.resolve(here, "../../src/oc_orch/entrypoints/web_assets");
const BUNDLE_PATH = path.join(ASSETS, "app.js");

export const tick = (ms = 0) => new Promise((r) => setTimeout(r, ms));

export async function waitFor(fn, { timeout = 5000, interval = 20 } = {}) {
  const start = Date.now();
  let lastError = null;
  for (;;) {
    try {
      const value = fn();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    if (Date.now() - start > timeout) {
      throw new Error(
        `waitFor 超时(${timeout}ms)${lastError ? `：${lastError.message}` : ""} ` +
        `predicate=${String(fn).slice(0, 140)}`);
    }
    await tick(interval);
  }
}

export class Deferred {
  constructor() {
    this.promise = new Promise((resolve, reject) => {
      this.resolve = (value) => { this.settled = true; resolve(value); };
      this.reject = reject;
    });
    this.settled = false;
  }
}

function responseOf(spec) {
  const s = spec === undefined ? {} : spec;
  const status = s.status ?? (s instanceof Error ? 500 : 200);
  const body = s instanceof Error || (s && "status" in s && !("body" in s))
    ? {ok: false, message: String(s?.message ?? s)}
    : s.body ?? s;
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => (typeof body === "function" ? body() : body),
  };
}

/**
 * 可编程路由表。键 `${METHOD} ${path}`；注册 pathname（无 query）时兼容任意查询串。
 * spec 可为对象 / {status, body} / Promise / 函数；缺失时 404（不把“没桩”当成功）。
 */
export class Routes {
  #map = new Map();
  #calls = [];

  set(method, pathname, spec) { this.#map.set(`${method} ${pathname}`, spec); }
  get(pathname, spec) { this.set("GET", pathname, spec); }
  post(pathname, spec) { this.set("POST", pathname, spec); }
  calls(predicate) { return this.#calls.filter(predicate ?? (() => true)); }
  count(predicate) { return this.calls(predicate).length; }

  handler() {
    return async (input, init = {}) => {
      const raw = typeof input === "string" ? input : String(input?.url ?? input);
      const [pathname = "", query = ""] = raw.includes("?") ? raw.split("?") : [raw, ""];
      const url = query ? `${pathname}?${query}` : pathname;
      const method = (init?.method || "GET").toUpperCase();
      this.#calls.push({method, url, path: pathname, query: query ? `?${query}` : "", init});
      let spec = this.#map.get(`${method} ${url}`) ?? this.#map.get(`${method} ${pathname}`);
      if (spec === undefined) {
        return responseOf({status: 404, body: {ok: false, message: "no stub", code: "NOT_FOUND"}});
      }
      if (typeof spec === "function") spec = spec({url, pathname, init});
      if (spec && typeof spec.then === "function") spec = await spec;
      return responseOf(spec);
    };
  }
}

export function coord(sid, overrides = {}) {
  return {
    session: sid, title: sid, counts: {}, gates: [], leases: [], projects: [],
    attention: "x", attention_rank: 6, created_at: 1, created_at_known: true,
    metadata_fresh: true, online: null, sources: [], identity: "unknown",
    ...overrides,
  };
}

export function snapshot(overrides = {}) {
  return {
    read_only: false,
    supervisor: {healthy: true, pid: 1, connected: true},
    service: {base: "http://127.0.0.1", version: "t"},
    tasks: [], leases: [], alerts: [], notifications: [], counts: {},
    ...overrides,
  };
}

export function baseRoutes() {
  const routes = new Routes();
  routes.get("/api/status", {ok: true, snapshot: snapshot()});
  routes.get("/api/coordinators", {ok: true, meta: {}, coordinators: []});
  routes.get("/api/audit?limit=60", {ok: true, audit: []});
  routes.get("/api/inputs?limit=50", {ok: true, inputs: []});
  routes.get("/api/input-watches?limit=50", {ok: true, watches: []});
  routes.get("/api/responsibilities?limit=200", {ok: true, responsibilities: []});
  // R2：refreshLight 每次同时拉业务健康与本地人工待办；缺失即 404 会让“恢复清除
  // 失败标记”类断言永久红，故基座提供空/良性默认。
  routes.get("/api/health", {ok: true, schema: "oc-orch.continuity-health.v1",
                             scanned: 0, process: {healthy: null, connected: null,
                             heartbeat_age_s: null, last_cycle_at: null},
                             semantics: "process-health-not-business-acceptance",
                             tasks: [], events: [],
                             coverage: {complete: true, next_after_id: null,
                                        cursor_advanced: false, scope: {}},
                             runtime: {}});
  routes.get("/api/health?limit=50", {body: {ok: true, schema: "oc-orch.continuity-health.v1",
                             scanned: 0, process: {healthy: null, connected: null,
                             heartbeat_age_s: null, last_cycle_at: null},
                             semantics: "process-health-not-business-acceptance",
                             tasks: [], events: [],
                             coverage: {complete: true, next_after_id: null,
                                        cursor_advanced: false, scope: {}},
                             runtime: {}}});
  routes.get("/api/incidents?limit=50", {body: {ok: true, incidents: [], status: {
    total: 0, by_state: {}, by_type: {}, recipient_refs: [], channel: "not-configured",
    email_configured: false, email_policy_enabled: false}}});
  return routes;
}

export function taskDetail(task = {}) {
  const {attempts: attemptsOverride, ...taskRest} = task;
  return {
    ok: true,
    task: {
      id: "T-1", project: "p", title: "任务标题", objective: "目标",
      acceptance: "标准", status: "running", coordinator_source: "explicit",
      effective_coordinator: "ses_c", current_attempt_id: "A-1",
      coordinator_session: "ses_c", ...taskRest,
    },
    attempts: attemptsOverride ?? [{
      id: "A-1", generation: 1, status: "result_received",
      dispatch_state: "confirmed", session_id: "ses_w",
      verification: {},
    }],
    attempts_full_count: 1, attempts_truncated: false, notifications: [],
  };
}

/* ---------------- 共享实例（每测试文件一个 jsdom） ---------------- */
let _dom = null;
let _win = null;
let _el = null;
let _mod = null;            // app.js 源码模块实例
let _state = null;          // state.js 的 state
let _refreshLight = null;   // data.js 的 refreshLight
let _manualRefresh = null;

const _elements = []; // 源码 boot 与打包 boot 创建的 oc-orch-app 实例
function trackElement(el) { if (!_elements.includes(el)) _elements.push(el); }
after(() => {
  // 应用 5s 轮询 ticker 是 Node 定时器（不随 jsdom.close 清理）；显式清除，
  // 保证去掉 --test-force-exit 后进程也能正常退出。
  for (const el of _elements) {
    try { clearInterval(el._ticker); el._ticker = null; } catch { /* ignore */ }
  }
  _elements.length = 0;
  _dom?.window.close();
});

function installGlobals(window) {
  const setGlobal = (key, value) => {
    try { globalThis[key] = value; }
    catch { /* Node 某些全局（如 navigator）是只读 getter，需显式重定义 */
      Object.defineProperty(globalThis, key, {value, configurable: true, writable: true});
    }
  };
  setGlobal("window", window);
  setGlobal("document", window.document);
  for (const key of ["customElements", "navigator", "location", "history",
                     "HTMLElement", "Element", "Event", "CustomEvent",
                     "KeyboardEvent", "MouseEvent", "Node", "DocumentFragment",
                     "Comment", "Text", "getComputedStyle", "FormData", "URL",
                     "URLSearchParams"]) {
    if (key in window) setGlobal(key, window[key]);
  }
  // api.js 依赖的 AbortController 由 Node 全局提供（jsdom 不实现）；
  // setTimeout 等保持 Node 原生：绑定到 window 会让 jsdom 内部定时器无限递归。
  setGlobal("requestAnimationFrame", (cb) => setTimeout(cb, 0));
  setGlobal("cancelAnimationFrame", (id) => clearTimeout(id));
}

function makeDialogs(win, options) {
  const {confirm: confirmValue, prompt: promptValue} = options;
  win.__dialogs = {confirms: [], prompts: []};
  win.confirm = (msg) => {
    win.__dialogs.confirms.push(String(msg));
    return confirmValue === undefined ? false : confirmValue;
  };
  win.prompt = (message, fallback) => {
    win.__dialogs.prompts.push(String(message));
    if (typeof promptValue === "function") return String(promptValue(message, fallback));
    return promptValue === undefined ? null : String(promptValue);
  };
  win.alert = () => {};
  globalThis.confirm = win.confirm;
  globalThis.prompt = win.prompt;
  globalThis.alert = win.alert;
}

function patchFetch(routes) {
  const fetcher = routes.handler();
  globalThis.fetch = _win.fetch = fetcher;
}

function resetState() {
  Object.assign(_state, {
    snap: null, audit: [], inputs: [], watches: [], resp: [], coordPages: {},
    unboundRows: [], unboundAfter: null, unboundFull: 0,
    unboundTruncated: false, unboundLock: false, unboundPages: 0,
    coordList: [], coordMap: {}, coordMeta: null,
    statusAt: 0, coordAt: 0, extraAt: 0,
    statusErr: false, coordErr: false, extraErr: false, detailErr: false,
    errors: {}, search: "", tab: {coordinator: "tasks"}, scroll: {},
    dirtyForm: false, auto: false, refreshing: false,
    turnCandidates: {},
    currentKey: null, pendingFocus: null,
  });
  _state.details.clear();
  _state.verifyForms.clear();
  _state.busyActions.clear();
  _state.policyDrafts.clear();
  _state.policyOperations.clear();
  _state.prototypeDrafts.clear();
  _state.prototypeSelected.clear();
  for (const key of Object.keys(_win.sessionStorage)) {
    if (key.startsWith("oc-orch-policy-pending:v1:")) _win.sessionStorage.removeItem(key);
  }
  _state.pendingRoute.clear();
}

/**
 * 启动（首个测试）或复位（后续测试）应用。返回句柄；每次调用都会复位状态。
 * @param {Routes} routes
 * @param {{hash?: string, confirm?: boolean, prompt?: string|Function}} options
 */
export async function boot(routes, options = {}) {
  const {hash = "#/"} = options;
  if (!_dom) {
    _dom = new JSDOM(
      `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"></head>` +
      `<body></body></html>`,
      {url: "http://127.0.0.1#/", pretendToBeVisual: true});
    _win = _dom.window;
    installGlobals(_win);
    makeDialogs(_win, options);
    patchFetch(routes);
    _mod = await import(path.join(SRC, "app.js"));
    if (_mod.OcOrchApp === undefined) throw new Error("app.js 未导出 OcOrchApp");
    const stateMod = await import(path.join(SRC, "state.js"));
    const dataMod = await import(path.join(SRC, "data.js"));
    _state = stateMod.state;
    _refreshLight = dataMod.refreshLight;
    _manualRefresh = dataMod.manualRefresh;
    if (_refreshLight === undefined) throw new Error("data.js 未导出 refreshLight");
    _el = document.createElement("oc-orch-app");
    document.body.appendChild(_el);
    trackElement(_el);
    await waitFor(() => _state.currentKey != null, {timeout: 8000});
  }
  makeDialogs(_win, options);
  patchFetch(routes);
  resetState();
  if (hash && _win.location.hash !== hash) _win.location.hash = hash;
  await _refreshLight();
  await tick(10);
  return {
    dom: _dom, window: _win, document: _win.document, el: _el, mod: _mod,
    state: _state, refreshLight: _refreshLight, manualRefresh: _manualRefresh,
    dialogs: _win.__dialogs,
  };
}

/* ---------------- 打包产物离线真实运行（每次独立隔离） ---------------- */
let bundleSeq = 0;

/**
 * 在全新 jsdom 中运行**随包分发的打包产物**（query-busting 隔离模块实例），
 * 验证的是真正随 wheel 分发的字节而非源码捷径。产物不导出 refreshLight，
 * 交互经 DOM（#refreshBtn / data-act 等）驱动。
 */
export async function bootBundle(routes, options = {}) {
  const {hash = "#/", shell = false} = options;
  // shell=true：以完整随包页面外壳（index.html）构造文档，应用从既有 <oc-orch-app>
  // 升级；用于验证“初始提示无残留、仅真实内容、toast 唯一”。
  const html = shell
    ? packagedShell()
    : `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"></head><body></body></html>`;
  const dom = new JSDOM(html, {url: `http://127.0.0.1${hash}`, pretendToBeVisual: true});
  const win = dom.window;
  installGlobals(win);
  makeDialogs(win, options);
  const fetcher = routes.handler();
  globalThis.fetch = win.fetch = fetcher;
  const bundleUrl = pathToFileURL(BUNDLE_PATH);
  bundleUrl.searchParams.set("boot", String(bundleSeq++));
  await import(bundleUrl.href);
  const el = shell
    ? document.querySelector("oc-orch-app")
    : document.createElement("oc-orch-app");
  if (!el) throw new Error("打包外壳缺少 <oc-orch-app>");
  if (el.parentNode !== document.body) document.body.appendChild(el);
  trackElement(el);
  if (typeof el.updateComplete?.then === "function") await el.updateComplete;
  return {dom, window: win, document: win.document, el, dialogs: win.__dialogs};
}

/* ---------------- 随包产物只读访问（资源/打包契约测试用） ---------------- */
export function packagedFiles() {
  return {
    shell: readFileSync(path.join(ASSETS, "index.html"), "utf8"),
    bundle: readFileSync(path.join(ASSETS, "app.js"), "utf8"),
    css: readFileSync(path.join(ASSETS, "app.css"), "utf8"),
  };
}
export function packagedBundle() { return packagedFiles().bundle; }
export function packagedShell() { return packagedFiles().shell; }
export function packagedCss() { return packagedFiles().css; }
export function packagedManifest() {
  return JSON.parse(readFileSync(path.join(ASSETS, "manifest.json"), "utf8"));
}

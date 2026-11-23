import {api, isNotFound} from "./api.js";
import {state, publish, mergeRows, updateCoordinators} from "./state.js";
import {parseHash, routeKey, routeGeneration, routeIsCurrent, needDetail, restoreNavigation} from "./router.js";
import {toast} from "./ui.js";

/* ============ 同步数据源（失败保留旧数据并标过期） ============ */
async function source(name, path, apply) {
  try {
    const body = await api(path);
    apply(body);
    delete state.errors[name];
    return true;
  } catch (error) {
    state.errors[name] = error.message;
    return false;
  }
}
export async function refreshStatus() {
  state.statusErr = !await source("status", "/api/status", body => {
    state.snap = body.snapshot;
    state.statusAt = Date.now();
  });
}
export async function refreshCoordinators() {
  state.coordErr = !await source("coordinators", "/api/coordinators", body => {
    updateCoordinators(body.coordinators || []);
    state.coordMeta = body.meta || {};
    state.coordAt = Date.now();
  });
}
export async function refreshExtras() {
  const results = await Promise.all([
    source("audit", "/api/audit?limit=60", body => { state.audit = body.audit || []; }),
    source("inputs", "/api/inputs?limit=50", body => { state.inputs = body.inputs || []; }),
    source("watches", "/api/input-watches?limit=50", body => { state.watches = body.watches || []; }),
    source("responsibilities", "/api/responsibilities?limit=200", body => { state.resp = body.responsibilities || []; }),
  ]);
  state.extraErr = results.some(ok => !ok);
  if (!state.extraErr) state.extraAt = Date.now();
}

/* ============ R2：业务健康 / 本地人工待办（只读；不推进 runtime 游标） ============ */
// 每次调用独立 GET /api/health（进程健康由服务端 supervisor 状态注入；客户端
// 只展示 health.runtime 的运行身份，不读 Git/版本号自我宣称同版）。
export async function refreshHealth(sidOrNull) {
  const sid = sidOrNull || null;
  const path = sid
    ? `/api/health?session_id=${encodeURIComponent(sid)}&limit=200`
    : "/api/health?limit=50";
  try {
    const body = await api(path);
    if (sid) state.healthBySid[sid] = {body, at: Date.now(), error: null};
    else state.health = {body, at: Date.now(), error: null};
    state.healthErr = false;
    state.healthAt = Date.now();
    return true;
  } catch (error) {
    const failure = {error, at: Date.now()};
    if (sid) {
      const prev = state.healthBySid[sid];
      state.healthBySid[sid] = {...failure, body: prev?.body ?? null};
    } else {
      state.health = {...failure, body: state.health?.body ?? null};
    }
    state.healthErr = true;
    return false;
  }
}

export async function refreshIncidents() {
  const ok = await source("incidents", "/api/incidents?limit=50", body => {
    state.incidents = body.incidents || [];
    state.incidentStatus = body.status || null;
  });
  state.incidentErr = !ok;
  return ok;
}

/* ============ 每 key 请求令牌：全局单调序号（杜绝 ABA 复用） ============
 * takeToken 每次取全局自增序号，任何时刻同一 key 只有当前（最大）序号请求能落地；
 * 完成时删除 current token 不泄漏 map，但下次从更大序号继续，绝不复用旧编号。 */
let tokenSeq = 0;
const detailTokens = new Map();
function takeToken(key) {
  const t = ++tokenSeq;
  detailTokens.set(key, t);
  return t;
}
const isLatest = (key, token) => detailTokens.get(key) === token;
function releaseTokenIfCurrent(key, token) {
  if (isLatest(key, token)) detailTokens.delete(key);
}

// 分页续页：按已加载页数有界重取（原子 replace 语义由调用方执行），
// 移走的对象从已加载范围内剔除，不丢分页游标；返回实际取到页数 fetched。
async function fetchPages(first, nextPath, pages) {
  let after = null, rows = [], full = null, firstBody = null, fetched = 0;
  for (let i = 0; i < pages; i++) {
    const path = i === 0 ? first : nextPath(after);
    const j = await api(path);
    fetched += 1;
    if (i === 0) firstBody = j;
    rows = mergeRows(rows, j.tasks || []);
    after = j.next_after_id != null ? j.next_after_id : null;
    full = j.tasks_full_count ?? j.full_count ?? full;
    if (after === null) break;
  }
  return {firstBody, rows, after, full, fetched};
}

// 详情数据主体：coordinator / unbound 按已加载页数有界重取第一页主体，task/attempt 单取。
async function fetchDetailBody(route, firstPath) {
  if (route.view === "coordinator") {
    const prev = state.coordPages[route.sid];
    const pages = Math.max(1, prev?.pages || 1);
    return fetchPages(
      firstPath,
      after => `/api/coordinator/${encodeURIComponent(route.sid)}?after_id=${encodeURIComponent(after)}&limit=200`,
      pages,
    );
  }
  if (route.unbound) {
    const pages = Math.max(1, state.unboundPages || 1);
    return fetchPages(
      firstPath,
      after => `/api/tasks/unbound?limit=100&after_id=${encodeURIComponent(after)}`,
      pages,
    );
  }
  return {firstBody: await api(firstPath)};
}

function maybeRestore(key, gen) {
  // 只在该路由仍是当前路由且是本次导航的等待目标时恢复一次；旧详情完成不会 focus 新路由。
  if (state.pendingFocus === key && routeIsCurrent(key, gen)) {
    state.pendingFocus = null;
    restoreNavigation(key);
  }
}

export function loadDetail(route = parseHash()) {
  const key = routeKey(route);
  if (!needDetail(route)) return Promise.resolve();
  const gen = routeGeneration();
  const token = takeToken(key); // 新导航总是可发新请求，token 唯一标识“这一代”
  state.pendingRoute.add(key);
  publish();
  const firstPath = route.unbound ? "/api/tasks/unbound?limit=100"
    : `/api/${route.view}/${encodeURIComponent(route.sid || route.id || "")}`;
  const previous = state.details.get(key);
  const pending = (async () => {
    try {
      const res = await fetchDetailBody(route, firstPath);
      // 过期响应（已有更新的请求，或已离开该路由）一律不落地。
      if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return null;
      // 导航采用“按已加载页数有界重取 + 原子 replace”，不 merge 旧 rows/after：
      // 回访时已移走/取消归属对象被剔除，after/truncated 由最新分页重算，
      // 不会出现“全载完后 after 回退到第一页游标、再次可点更多”的矛盾。
      if (route.view === "coordinator") {
        state.coordPages[route.sid] = {
          tasks: res.rows,
          full: res.full ?? res.rows.length,
          after: res.after,
          truncated: res.after !== null,
          lock: false,
          pages: res.fetched,
        };
      } else if (route.unbound) {
        state.unboundRows = res.rows;
        state.unboundAfter = res.after;
        state.unboundFull = res.full ?? res.rows.length;
        state.unboundTruncated = res.after !== null;
        state.unboundPages = res.fetched;
      }
      state.details.set(key, {body: res.firstBody, at: Date.now(), error: null});
      delete state.errors[key];
      return {body: res.firstBody, at: Date.now(), error: null};
    } catch (error) {
      if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return null;
      // 404：明确对象缺失，不继续展示可写的旧对象；网络/超时：保留旧数据并标过期。
      const notFound = isNotFound(error);
      state.details.set(key, {
        body: notFound ? null : (previous?.body ?? null),
        at: previous?.at || 0,
        error,
        notFound,
      });
      state.errors[key] = error.message;
      return {body: null, at: previous?.at || 0, error, notFound};
    } finally {
      // 只有最新一代才能清理 token/pendingRoute/恢复滚动：旧 finally 不得删新 pending。
      if (isLatest(key, token)) {
        detailTokens.delete(key);
        state.pendingRoute.delete(key);
        maybeRestore(key, gen);
        publish();
      }
    }
  })();
  return pending;
}

/* ============ 后台静默刷新（保留旧 DOM，不显示加载、不抢 focus/scroll） ============ */
export async function refreshCoordinatorDetail(sid) {
  const key = `coordinator:${sid}`;
  const pag = state.coordPages[sid];
  if (!pag || pag.lock) return;
  const token = takeToken(key);
  const gen = routeGeneration();
  const prev = state.details.get(key);
  pag.lock = true;
  try {
    const pages = Math.max(1, pag.pages || 1);
    const {firstBody, rows, after, full} = await fetchPages(
      `/api/coordinator/${encodeURIComponent(sid)}?limit=200`,
      after => `/api/coordinator/${encodeURIComponent(sid)}?after_id=${encodeURIComponent(after)}&limit=200`,
      pages,
    );
    // 导航已发出新请求或已离开该路由：丢弃本次后台旧响应，避免覆盖当前视图。
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    pag.tasks = rows;
    pag.after = after;
    pag.full = full ?? rows.length;
    pag.truncated = after !== null;
    pag.pages = pages;
    state.details.set(key, {body: firstBody, at: Date.now(), error: null});
    delete state.errors[key];
  } catch (error) {
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    const notFound = isNotFound(error);
    state.details.set(key, {
      body: notFound ? null : (prev?.body ?? null),
      at: prev?.at || 0,
      error,
      notFound,
    });
    state.errors[key] = error.message;
  } finally {
    pag.lock = false;
    releaseTokenIfCurrent(key, token);
  }
}

export async function refreshUnbound() {
  if (state.unboundLock) return;
  const key = "tasks:unbound";
  const token = takeToken(key);
  const gen = routeGeneration();
  const prev = state.details.get(key);
  state.unboundLock = true;
  try {
    const pages = Math.max(1, state.unboundPages || 1);
    const {firstBody, rows, after, full} = await fetchPages(
      "/api/tasks/unbound?limit=100",
      after => `/api/tasks/unbound?limit=100&after_id=${encodeURIComponent(after)}`,
      pages,
    );
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    state.unboundRows = rows;
    state.unboundAfter = after;
    state.unboundFull = full ?? rows.length;
    state.unboundTruncated = after !== null;
    state.unboundPages = pages;
    state.details.set(key, {body: firstBody, at: Date.now(), error: null});
    delete state.errors[key];
  } catch (error) {
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    const notFound = isNotFound(error);
    state.details.set(key, {
      body: notFound ? null : (prev?.body ?? null),
      at: prev?.at || 0,
      error,
      notFound,
    });
    state.errors[key] = error.message;
  } finally {
    state.unboundLock = false;
    releaseTokenIfCurrent(key, token);
  }
}

export async function refreshSimpleDetail(route = parseHash()) {
  const key = routeKey(route);
  const token = takeToken(key);
  const gen = routeGeneration();
  const prev = state.details.get(key);
  try {
    const body = await api(`/api/${route.view}/${encodeURIComponent(route.id || "")}`);
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    state.details.set(key, {body, at: Date.now(), error: null});
    delete state.errors[key];
  } catch (error) {
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    const notFound = isNotFound(error);
    state.details.set(key, {
      body: notFound ? null : (prev?.body ?? null),
      at: prev?.at || 0,
      error,
      notFound,
    });
    state.errors[key] = error.message;
  } finally {
    releaseTokenIfCurrent(key, token);
  }
}

// 后台静默刷新入口：无 body / coordinator 无 pag（首次加载 500/超时/404 后）时，
// 改走 loadDetail 恢复（pendingRoute 去重），让“重试”真正能重发首载请求。
export function refreshDetailSilent(route = parseHash()) {
  const key = routeKey(route);
  if (state.pendingRoute.has(key)) return Promise.resolve(); // 已有请求在途，避免叠加
  if (route.view === "coordinator") {
    if (!state.coordPages[route.sid] || !state.details.get(key)?.body) return loadDetail(route);
    return refreshCoordinatorDetail(route.sid);
  }
  if (route.unbound) {
    if (!state.unboundPages || !state.details.get(key)?.body) return loadDetail(route);
    return refreshUnbound();
  }
  if (!state.details.get(key)?.body) return loadDetail(route);
  return refreshSimpleDetail(route);
}

/* ============ 分页加载更多（续页；带代次/token/pag 身份守卫） ============ */
export async function loadMoreCoord(sid) {
  const key = `coordinator:${sid}`;
  // 导航在途（pendingRoute 未清）时禁止续页抢同 key 请求令牌：
  // 否则 loadMore 取新 token 会使导航 finally 视为非最新而不清理 pendingRoute，
  // loadMore finally 又不清，导致该详情被全局刷新永久跳过。入口直返，不动 lock/token。
  if (state.pendingRoute.has(key)) return;
  const pag = state.coordPages[sid];
  if (!pag || pag.lock || !pag.after) return;
  const gen = routeGeneration();
  const token = takeToken(key);
  pag.lock = true;
  publish();
  try {
    const j = await api(`/api/coordinator/${encodeURIComponent(sid)}?after_id=${encodeURIComponent(pag.after)}&limit=200`);
    // 三重守卫：A→B→A 回旧页或导航原子替换 pag 对象后，丢弃过期续页结果，避免污染当前视图。
    if (!isLatest(key, token) || !routeIsCurrent(key, gen) || state.coordPages[sid] !== pag) return;
    pag.tasks = mergeRows(pag.tasks, j.tasks || []);
    pag.truncated = !!j.tasks_truncated;
    pag.after = j.next_after_id != null ? j.next_after_id : null;
    if (j.tasks_full_count != null) pag.full = j.tasks_full_count;
    pag.pages += 1;
  } catch (error) {
    if (state.coordPages[sid] === pag) toast(`加载失败：${error.message}`);
  } finally {
    releaseTokenIfCurrent(key, token);
    if (state.coordPages[sid] === pag) {
      pag.lock = false;
      publish();
    }
  }
}

export async function loadMoreUnbound() {
  // 同 loadMoreCoord：导航在途时续页不得抢同 key 请求令牌，避免 pendingRoute 被永久遗留。
  if (state.pendingRoute.has("tasks:unbound")) return;
  if (state.unboundLock || !state.unboundAfter) return;
  const key = "tasks:unbound";
  const gen = routeGeneration();
  const token = takeToken(key);
  state.unboundLock = true;
  publish();
  try {
    const j = await api(`/api/tasks/unbound?limit=100&after_id=${encodeURIComponent(state.unboundAfter)}`);
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    state.unboundRows = mergeRows(state.unboundRows, j.tasks || []);
    state.unboundTruncated = !!j.truncated;
    state.unboundAfter = j.next_after_id || null;
    if (j.full_count != null) state.unboundFull = j.full_count;
    state.unboundPages += 1;
  } catch (error) {
    if (isLatest(key, token)) toast(`加载失败：${error.message}`);
  } finally {
    state.unboundLock = false;
    releaseTokenIfCurrent(key, token);
    publish();
  }
}

/* ============ 刷新编排 ============ */
let inFlight;
export function refreshLight() {
  if (inFlight) return inFlight;
  state.refreshing = true;
  publish();
  const route = parseHash();
  inFlight = (async () => {
    try {
      const jobs = [refreshStatus(), refreshCoordinators(), refreshExtras()];
      const key = routeKey(route);
      // R2：业务健康按当前视图取数（工作台按 session 过滤、诊断取全局），
      // 本地人工待办每次刷新一并取（只读；失败保留旧数据标过期）。
      if (route.view === "coordinator") jobs.push(refreshHealth(route.sid));
      else if (route.view === "diagnostics") jobs.push(refreshHealth(null));
      jobs.push(refreshIncidents());
      // 后台刷新：不先加载中；详情已有分页/缓存时静默有界重取；首载缺失由
      // refreshDetailSilent 走 loadDetail 恢复（去重由 pendingRoute 保证）。
      if (!state.pendingRoute.has(key) && needDetail(route)) {
        jobs.push(refreshDetailSilent(route));
      }
      await Promise.all(jobs);
    } finally {
      inFlight = null;
      state.refreshing = false;
      publish();
    }
  })();
  return inFlight;
}

export async function manualRefresh() {
  await refreshLight();
  const anyErr = state.statusErr || state.coordErr || state.extraErr || Object.keys(state.errors).length > 0;
  toast(anyErr ? "部分刷新失败，已保留旧数据" : "数据已刷新");
}

// 保留历史 import 兼容（旧页面导出名）。
export {mergeRows};
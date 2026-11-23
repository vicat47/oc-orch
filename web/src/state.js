// 只保存展示事实，不在浏览器推导验收/ACK/写权结论。
export const state = {
  snap: null, audit: [], inputs: [], watches: [], resp: [], coordPages: {},
  unboundRows: [], unboundAfter: null, unboundFull: 0,
  unboundTruncated: false, unboundLock: false, unboundPages: 0,
  coordList: [], coordMap: {}, coordMeta: null, details: new Map(),
  statusAt: 0, coordAt: 0, extraAt: 0,
  statusErr: false, coordErr: false, extraErr: false, detailErr: false,
  errors: {}, search: "", tab: {coordinator: "tasks"}, scroll: {},
  dirtyForm: false, auto: true, refreshing: false,
  verifyForms: new Map(), busyActions: new Set(),
  // R2 业务健康/本地人工待办：只保存展示事实，不推导验收/发送结论。
  health: null, healthBySid: {}, healthErr: false, healthAt: 0,
  incidents: [], incidentStatus: null, incidentErr: false,
  // 回合 hold 的只读候选/操作结果；按 session 保存，不据此推断可发送。
  turnCandidates: {},
  // 验收表单草稿：输入事件同步记录，提交读取草稿而非易失 DOM，刷新换节点不丢值。
  verifyDrafts: new Map(),
  policyDrafts: new Map(), policyOperations: new Map(),
  prototypeDrafts: new Map(), prototypeSelected: new Map(),
  currentKey: null, pendingFocus: null, pendingRoute: new Set(),
  // 应用初始化标志：boot 完成首轮取数/路由前为 false，详情首屏（deep-link）显示加载而非错误。
  ready: false,
};
const listeners = new Set();
export function subscribe(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}
export function publish() {
  for (const listener of listeners) listener(state);
}
export const ro = () => !!state.snap?.read_only;

export function updateCoordinators(incoming = []) {
  const fresh = new Set(incoming.map(c => c.session));
  const seen = new Set(state.coordList);
  for (const c of incoming) {
    if (!seen.has(c.session)) {
      state.coordList.push(c.session);
      seen.add(c.session);
    }
    state.coordMap[c.session] = {...state.coordMap[c.session], ...c};
  }
  for (const sid of state.coordList) state.coordMap[sid].missing_snapshot = !fresh.has(sid);
}

export function mergeRows(previous = [], incoming = []) {
  const rows = new Map([...previous, ...incoming].map(row => [row.id, row]));
  return [...rows.values()].sort((a, b) => a.id.localeCompare(b.id));
}

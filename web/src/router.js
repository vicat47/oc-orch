import {state, publish} from "./state.js";

let generation = 0;
export function parseHash() {
  const parts = location.hash.replace(/^#\/?/, "").split("/").filter(Boolean);
  try {
    if (parts[0] === "tasks") return {view: "tasks", unbound: parts[1] === "unbound"};
    if (parts[0] === "diagnostics") return {view: "diagnostics"};
    if (parts[0] === "coordinator") return {view: "coordinator", sid: decodeURIComponent(parts[1] || "")};
    if (["task", "attempt"].includes(parts[0])) return {view: parts[0], id: decodeURIComponent(parts[1] || "")};
  } catch { /* 错误的编码按总览处理，不中止应用。 */ }
  return {view: "overview"};
}
export const routeKey = (route = parseHash()) =>
  `${route.view}:${route.sid || route.id || (route.unbound ? "unbound" : "")}`;
export const needDetail = route =>
  ["coordinator", "task", "attempt"].includes(route.view) || !!route.unbound;
export const routeGeneration = () => generation;
export function routeIsCurrent(key, version) {
  return routeKey() === key && generation === version;
}
export function navigate() {
  if (state.currentKey) state.scroll[state.currentKey] = window.scrollY;
  generation += 1;
  state.currentKey = routeKey();
  state.detailErr = false;
  publish();
}
// 只导航时调用一次：旧详情完成也带 key/代次校验，不会 focus 到新路由。
export function restoreNavigation(key) {
  requestAnimationFrame(() => {
    if (!key || routeKey() !== key) return;
    const main = document.getElementById("main");
    if (!main) return;
    window.scrollTo(0, state.scroll[key] || 0);
    main.focus({preventScroll: true});
  });
}
/**
 * 测试辅助：fake-fetch 与假 host（仅供 node --test 离线验证；不构成真实集成）。
 */

/** 创建可注入 bridge 的 fake-fetch：按 URL/METHOD 路由，记录每次调用。 */
export function createFetch() {
  const calls = []
  const handlers = []
  const fetchImpl = async (url, init = {}) => {
    const entry = { url: String(url), method: init.method ?? "GET", init, startedAt: Date.now() }
    calls.push(entry)
    for (const h of handlers) {
      if (h.match(url, init)) {
        entry.handledBy = h.name
        return h.fn(url, init, entry)
      }
    }
    throw new TypeError("fetch failed (未匹配路由)")
  }
  return {
    fetch: fetchImpl,
    calls,
    /** match(url, init) -> bool；name 便于调试；fn(url, init, entry) -> Response */
    when(match, name, fn) {
      handlers.push({ match, name, fn })
      return this
    },
    /** 命中次数统计 */
    count(predicate = () => true) {
      return calls.filter(predicate).length
    },
  }
}

export const jsonBody = (value, status = 200) =>
  new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  })

export const notFound = () => jsonBody({ ok: false, code: "NOT_FOUND", message: "not found" }, 404)

/** 标准策略视图（GET /api/delivery-policy 返回）。可覆盖字段。 */
export function makeView(overrides = {}) {
  return {
    ok: true,
    schema: "oc-orch.delivery-policy.v1",
    scope: "task",
    scope_id: "T-1",
    project: "proj-a",
    managed: true,
    revision: 3,
    read_only: false,
    version: {
      policy: {
        preset: "budget-first",
        stage: "prototype",
        limits: { max_parallel: 2, max_candidates: 4, max_challenge_rounds: 1 },
      },
    },
    instructions: "下一步：等待验收并按共同底线核对",
    execution_enforced: false,
    coverage: {},
    runtime: {
      release_id: "rel-1",
      manifest_verified: true,
      writer_contract: "continuity-v2",
      writer_guard_valid: true,
      delivery_policy_guard_valid: true,
    },
    instruction_target: { session_id: "ses-1", task_id: "T-1" },
    ...overrides,
  }
}

/** 标准配置。writesEnabled 默认 false（禁写），expected 需显式提供能力表。 */
export function makeConfig(overrides = {}) {
  return {
    baseUrl: "http://127.0.0.1:8787",
    projectScope: "proj-a",
    taskScope: "T-1",
    sessionID: "ses-1",
    writesEnabled: true,
    expected: {
      writerContract: "continuity-v2",
      requireManifest: true,
    },
    ...overrides,
  }
}

/** 挂 GET /api/delivery-policy 路由到 fake-fetch。 */
export function routePolicyRead(ff, { view = makeView(), status = 200 } = {}) {
  ff.when(
    (url) => {
      const u = new URL(url)
      return u.pathname === "/api/delivery-policy" && u.searchParams.has("task_id")
    },
    "policy-read",
    () => jsonBody(view, status),
  )
}

/** 挂 POST /api/delivery-policy/:kind 路由；fn 可返回 Response 或抛出。 */
export function routePolicyWrite(ff, kind, fn) {
  ff.when((url) => {
    const u = new URL(url)
    return u.pathname === `/api/delivery-policy/${kind}`
  }, `policy-write-${kind}`, async (url, init, entry) => {
    entry.body = JSON.parse(String(init.body ?? "{}"))
    const response = await fn(url, init, entry)
    if (response instanceof Response) {
      try {
        const body = await response.clone().json()
        // 对齐真实API必带原operation_id；显式错误ID反例不被修正。
        if (body.ok === true && !Object.hasOwn(body, "operation_id")) {
          return jsonBody({...body, operation_id: entry.body.operation_id}, response.status)
        }
      } catch { /* 非JSON故障原样返回 */ }
    }
    return response
  })
}

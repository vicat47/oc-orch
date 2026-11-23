/**
 * oc-orch 交付策略薄客户端（纯逻辑，零运行时依赖）
 *
 * 本模块不 import OpenCode SDK；SDK 由宿主在 TUI/GUI 入口按官方规范
 * （@opencode/plugin/tui、@opencode/plugin/gui，V2 Plugin.define）运行时解析。
 * 本模块只依赖 Web 标准能力（fetch / AbortController / crypto），离线可用
 * fake-fetch 以 node --test 验证。
 *
 * API 契约（父级已按原契约落地 GET /api/delivery-policy 与 POST task/project，并通过 126 定向/完整 1982 及 Web 真 Chromium 四视口；本模块按此实现。以下形状为契约依据，本模块未做真实服务对拍，真实验证由父完成）：
 *   GET  /api/delivery-policy?task_id=T | ?project=p
 *     -> {ok:true, schema:'oc-orch.delivery-policy.v1', scope, scope_id,
 *         project, managed, revision, read_only?:bool,
 *         version:{policy:{preset,stage,limits},...},
 *         instructions:string|null, execution_enforced:bool, coverage:object,
 *         runtime:{release_id, manifest_verified, writer_contract,
 *                  writer_guard_valid, delivery_policy_guard_valid},
 *         instruction_target?:{session_id,task_id}
 *   真实边界（父级核验）：runtime.writer_contract 仍 continuity-v2；
 *   runtime.delivery_policy_guard_valid 表示独立 delivery-policy-v1 契约；
 *   未 manifest 仍拒插件写；GET 有 read_only 字段。
 *   POST /api/delivery-policy/task    body {task_id, policy|inherit:true,
 *         expected_revision, operation_id, reason, confirm:task_id}
 *   POST /api/delivery-policy/project body {project, policy,
 *         expected_revision, operation_id, reason, confirm:project}
 *     -> {ok, applied, applied_revision, current:<GET 相同视图>, execution_enforced}
 *
 * 安全不变量（本模块实现，tests 离线验证）：
 * - 未配置 baseUrl 时自身零 I/O，绝不默认 8787；scope/id 一律显式，不猜测。
 * - 默认禁写；写前做能力/身份严格匹配（expected 能力表）与 CAS expected_revision。
 * - fetch 拒 redirect / 非环回 / 带凭据 URL。
 * - 未知结果保原 operation_id/body，禁止自动重投；仅可显式原体复核（replay）。
 * - 取消/卸载中止读并忽略晚到结果；已发 POST 的取消按“结果未知”处理，不算没执行。
 * - 回执展示以 response.current 真实快照为准，不按动作名/回执外壳断言。
 */

export const SCHEMA = "oc-orch.delivery-policy.v1"
export const PRESETS = Object.freeze(["budget-first", "elastic", "latency-first"])
export const STAGES = Object.freeze(["standard", "prototype"])
export const LIMIT_RANGES = Object.freeze({
  max_parallel: [1, 32],
  max_candidates: [1, 8],
  max_challenge_rounds: [0, 8],
})

export class BridgeError extends Error {
  constructor(code, message, detail = null) {
    super(message)
    this.name = "BridgeError"
    this.code = code
    this.detail = detail
  }
}

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "::1"])

/** 环回判断（hostname 去掉 [] 后小写比较）。 */
export function isLoopbackHost(hostname) {
  if (typeof hostname !== "string") return false
  const h = hostname.trim().toLowerCase().replace(/^\[|\]$/g, "")
  return LOOPBACK_HOSTS.has(h)
}

/** 校验 baseUrl：仅 http/https、环回主机、无凭据。非法抛 BridgeError。 */
export function normalizeBaseUrl(raw) {
  if (typeof raw !== "string" || raw.trim() === "") {
    throw new BridgeError("BAD_BASE_URL", "baseUrl 必须为非空字符串")
  }
  let url
  try {
    url = new URL(raw)
  } catch {
    throw new BridgeError("BAD_BASE_URL", "baseUrl 无法解析")
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new BridgeError("BAD_BASE_URL", "baseUrl 仅允许 http/https")
  }
  if (!isLoopbackHost(url.hostname)) {
    throw new BridgeError("BAD_BASE_URL",
      "baseUrl 必须为环回地址（127.0.0.1 / localhost / ::1）")
  }
  if (url.username !== "" || url.password !== "") {
    throw new BridgeError("BAD_BASE_URL", "baseUrl 不允许携带凭据")
  }
  return url.href.replace(/\/+$/, "")
}

/**
 * 规范化插件配置。
 * - baseUrl 非法时置空并记录错误 -> 保持零 I/O，不默认 8787。
 * - expected：写前置能力表；未提供时写被 CAPABILITY_NOT_CONFIGURED 拒绝。
 * - projectScope/taskScope/sessionID 只是 UI 默认值；每次读/写仍要求显式 scope+id。
 * - writesEnabled 默认 false（默认禁写）。
 * - allowGuiWrites 默认 false：GUI/浏览器直连本机 oc-orch 存在跨源/预检限制
 *   （oc-orch Web 仅同源 Origin+CSRF，未启 CORS/OPTIONS），真实同源通道未核验
 *   前 GUI 写命令保持不可用。
 * - expected.requireManifest 默认 true：未 manifest 仍拒插件写（对齐服务端边界）。
 */
export function normalizeConfig(raw = {}) {
  const errors = []
  let baseUrl = null
  if (raw.baseUrl != null && raw.baseUrl !== "") {
    try {
      baseUrl = normalizeBaseUrl(raw.baseUrl)
    } catch (e) {
      errors.push(e.message)
    }
  }
  const projectScope =
    typeof raw.projectScope === "string" && raw.projectScope !== "" ? raw.projectScope.trim() : null
  const taskScope =
    typeof raw.taskScope === "string" && raw.taskScope !== "" ? raw.taskScope.trim() : null
  const sessionID =
    typeof raw.sessionID === "string" && raw.sessionID !== "" ? raw.sessionID.trim() : null
  const writesEnabled = raw.writesEnabled === true
  const allowGuiWrites = raw.allowGuiWrites === true
  const timeoutMs =
    Number.isInteger(raw.timeoutMs) && raw.timeoutMs > 0 ? raw.timeoutMs : 8000
  let expected = null
  if (raw.expected != null) {
    if (typeof raw.expected !== "object" || Array.isArray(raw.expected)) {
      errors.push("expected 必须是对象")
    } else {
      const writerContract =
        typeof raw.expected.writerContract === "string" && raw.expected.writerContract.trim() !== ""
          ? raw.expected.writerContract
          : null
      if (!writerContract) errors.push("expected.writerContract 必须为非空字符串（写前置能力）")
      expected = {
        writerContract,
        // 未 manifest 仍拒插件写（对齐服务端边界）；显式 requireManifest:false 才放宽
        requireManifest: raw.expected.requireManifest !== false,
        releaseId: typeof raw.expected.releaseId === "string" ? raw.expected.releaseId : null,
        requireWriterGuard: raw.expected.requireWriterGuard !== false,
        requireDeliveryPolicyGuard: raw.expected.requireDeliveryPolicyGuard !== false,
      }
    }
  }
  const config = {
    baseUrl, projectScope, taskScope, sessionID, writesEnabled, allowGuiWrites, timeoutMs, expected,
  }
  return { ok: errors.length === 0, errors, config }
}

/** 稳定序列化（键排序），用于相同 operation_id 的体冲突检测。 */
export function fingerprintBody(value) {
  return JSON.stringify(sortObject(value ?? {}))
}

function sortObject(value) {
  if (Array.isArray(value)) return value.map(sortObject)
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value).sort().map((k) => [k, sortObject(value[k])]),
    )
  }
  return value
}

/** 生成 operation_id（无 crypto 时限用时间戳兜底）。 */
export function makeOperationId() {
  if (typeof globalThis.crypto?.randomUUID === "function") return globalThis.crypto.randomUUID()
  return `op-${Date.now()}-${Math.random().toString(36).slice(2)}`
}

// ---- operation 台账（模块级单例：同进程多窗口共享，近似并发去重） ----
// 真实跨窗口/跨进程去重以服务端 operation 幂等 + CAS 为最终守卫（离线不可验）。
export function createOperationLedger() {
  const state = new Map()
  return {
    /** 登记意图；同 ID 不同体 -> 冲突（不发送）。同 ID 同体 -> 允许（显式复核/重放）。 */
    begin(operationId, fingerprint) {
      const existing = state.get(operationId)
      if (existing && existing.fingerprint !== fingerprint) {
        return {
          ok: false,
          code: "OPERATION_BODY_CONFLICT",
          message: `相同 operation_id 已被不同请求体占用：${operationId}`,
        }
      }
      state.set(operationId, { operationId, fingerprint, status: "pending", createdAt: Date.now() })
      return { ok: true }
    },
    record(operationId, entry) {
      const prev = state.get(operationId) ?? {}
      state.set(operationId, { ...prev, ...entry, operationId })
    },
    get(operationId) {
      return state.get(operationId)
    },
    listUnknown() {
      return [...state.values()].filter((e) => e.status === "unknown")
    },
    reset() {
      state.clear()
    },
  }
}

let sharedLedger = createOperationLedger()
export function getSharedLedger() {
  return sharedLedger
}
export function resetOperationLedgerForTests() {
  sharedLedger = createOperationLedger()
}

// ---- 视图解析 / 能力匹配 / 摘要（纯函数，可在 node --test 验证） ----

/** 校验 GET 视图；字段缺失/类型错误一律 fail-closed。 */
export function parsePolicyView(data) {
  if (!data || typeof data !== "object" || data.ok !== true) {
    return { ok: false, code: "NOT_OK", message: "视图 ok!=true" }
  }
  if (data.schema !== SCHEMA) {
    return { ok: false, code: "SCHEMA_MISMATCH", message: `schema 不是 ${SCHEMA}` }
  }
  if (data.scope !== "task" && data.scope !== "project") {
    return { ok: false, code: "BAD_SCOPE", message: "scope 必须为 task|project" }
  }
  if (typeof data.scope_id !== "string" || data.scope_id === "") {
    return { ok: false, code: "BAD_SCOPE_ID", message: "scope_id 缺失" }
  }
  if (typeof data.managed !== "boolean") {
    return { ok: false, code: "BAD_MANAGED", message: "managed 缺失" }
  }
  if (!Number.isInteger(data.revision) || data.revision < 0) {
    return { ok: false, code: "BAD_REVISION", message: "revision 必须为 int>=0" }
  }
  if (typeof data.execution_enforced !== "boolean") {
    return { ok: false, code: "BAD_ENFORCED", message: "execution_enforced 缺失" }
  }
  // GET 视图含 read_only（父级核验）。可选但若非布尔则拒绝，避免 UI 误判可写。
  if (data.read_only != null && typeof data.read_only !== "boolean") {
    return { ok: false, code: "BAD_READ_ONLY", message: "read_only 必须为布尔" }
  }
  if (!data.managed) {
    if (data.revision !== 0 || data.version !== null || data.execution_enforced !== false) {
      return { ok: false, code: "BAD_VERSION", message: "未纳管视图必须为revision=0/version=null/未执行" }
    }
    return { ok: true, view: data }
  }
  if (data.revision < 1 || !data.version || typeof data.version !== "object") {
    return { ok: false, code: "BAD_VERSION", message: "version 缺失" }
  }
  const policy = data.version.policy ?? {}
  if (!PRESETS.includes(policy.preset)) {
    return { ok: false, code: "BAD_PRESET", message: "preset 非法" }
  }
  if (!STAGES.includes(policy.stage)) {
    return { ok: false, code: "BAD_STAGE", message: "stage 非法" }
  }
  if (Object.keys(policy).sort().join(",") !== "limits,preset,stage"
      || !policy.limits || typeof policy.limits !== "object"
      || Object.keys(policy.limits).sort().join(",") !== Object.keys(LIMIT_RANGES).sort().join(",")
      || Object.entries(LIMIT_RANGES).some(([key, [lo, hi]]) =>
        !Number.isSafeInteger(policy.limits[key]) || policy.limits[key] < lo || policy.limits[key] > hi)) {
    return { ok: false, code: "BAD_LIMITS", message: "策略字段/独立上限缺失或非法" }
  }
  return { ok: true, view: data }
}

/** 写前能力/身份严格匹配（基于最近一次 GET 视图 + 本地 expected 能力表）。 */
export function assertWritePreconditions(view, expected) {
  if (!expected) {
    return {
      ok: false,
      code: "CAPABILITY_NOT_CONFIGURED",
      message: "未配置 expected 能力表（含 writerContract），拒绝写",
    }
  }
  if (!view || typeof view !== "object") {
    return { ok: false, code: "CAPABILITY_UNKNOWN", message: "缺少策略视图，无法执行写前能力/身份匹配" }
  }
  const runtime = view.runtime ?? {}
  if (runtime.writer_contract !== expected.writerContract) {
    return {
      ok: false,
      code: "CAPABILITY_MISMATCH",
      message: `writer_contract 不匹配（期望 ${expected.writerContract}，实际 ${JSON.stringify(runtime.writer_contract)}）`,
    }
  }
  if (expected.requireWriterGuard && runtime.writer_guard_valid !== true) {
    return { ok: false, code: "CAPABILITY_MISMATCH", message: "writer_guard_valid 非 true，拒绝写" }
  }
  if (expected.requireDeliveryPolicyGuard && runtime.delivery_policy_guard_valid !== true) {
    return {
      ok: false,
      code: "CAPABILITY_MISMATCH",
      message: "delivery_policy_guard_valid 非 true，拒绝写",
    }
  }
  if (expected.requireManifest && runtime.manifest_verified !== true) {
    return { ok: false, code: "CAPABILITY_MISMATCH", message: "manifest_verified 非 true，拒绝写" }
  }
  if (expected.releaseId != null && runtime.release_id !== expected.releaseId) {
    return {
      ok: false,
      code: "IDENTITY_MISMATCH",
      message: `release_id 不匹配（期望 ${expected.releaseId}，实际 ${JSON.stringify(runtime.release_id)}）`,
    }
  }
  return { ok: true }
}

/** 供 UI 展示的脱敏摘要（不含原始证据/邮件正文/台账正文）。 */
export function summarizeView(view) {
  const policy = view.version?.policy ?? {}
  return {
    scope: view.scope,
    scope_id: view.scope_id,
    project: typeof view.project === "string" ? view.project : null,
    managed: view.managed,
    revision: view.revision,
    read_only: view.read_only === true,
    preset: policy.preset ?? "(未设置)",
    stage: policy.stage ?? "(未设置)",
    limits: policy.limits ?? {},
    instructions: typeof view.instructions === "string" ? view.instructions : null,
    execution_enforced: view.execution_enforced === true,
    release_id: view.runtime?.release_id ?? null,
    manifest_verified: view.runtime?.manifest_verified === true,
    writer_contract: view.runtime?.writer_contract ?? null,
    writer_guard_valid: view.runtime?.writer_guard_valid === true,
    delivery_policy_guard_valid: view.runtime?.delivery_policy_guard_valid === true,
  }
}

async function safeJson(resp) {
  try {
    if (typeof resp.json !== "function") return undefined
    return await resp.json()
  } catch {
    return undefined
  }
}

// ---- 薄客户端 ----

export class DeliveryBridge {
  /**
   * @param {object} config normalizeConfig 产出
   * @param {object} opts { fetcher?, ledger? } 可注入 fake（node --test）
   */
  constructor(config, { fetcher = fetch, ledger = sharedLedger } = {}) {
    this.config = config
    this.fetcher = fetcher
    this.ledger = ledger
    this._controller = new AbortController()
    this._disposed = false
    this._stats = { fetches: 0, writesSent: 0 }
    this._confirmedRecords = new WeakSet()
  }

  get disposed() {
    return this._disposed
  }

  /** 已配置 baseUrl 才算“可能联网”；未配置时所有方法零 I/O。 */
  get configured() {
    return this.config.baseUrl != null && this.config.baseUrl !== ""
  }

  stats() {
    return { fetches: this._stats.fetches, writesSent: this._stats.writesSent }
  }

  /** 卸载：中止全部在途请求；之后读/写返回 DISPOSED，不再发起新请求。 */
  dispose() {
    if (this._disposed) return
    this._disposed = true
    this._controller.abort()
  }

  _assertNotDisposed() {
    if (this._disposed) {
      return { ok: false, code: "DISPOSED", message: "bridge 已卸载（reload/unload）" }
    }
    return { ok: true }
  }

  /** 在途信号 = 卸载信号 ∪ 调用方信号 ∪ 超时。 */
  _acquireSignal(callerSignal) {
    const ac = new AbortController()
    let timer
    const relay = () => ac.abort()
    if (this._controller.signal.aborted) ac.abort()
    else this._controller.signal.addEventListener("abort", relay, { once: true })
    if (callerSignal && typeof callerSignal.addEventListener === "function") {
      if (callerSignal.aborted) ac.abort()
      else callerSignal.addEventListener("abort", relay, { once: true })
    }
    timer = setTimeout(() => ac.abort(), this.config.timeoutMs)
    const release = () => {
      clearTimeout(timer)
      this._controller.signal.removeEventListener("abort", relay)
      if (callerSignal?.removeEventListener) callerSignal.removeEventListener("abort", relay)
    }
    return { signal: ac.signal, release }
  }

  /**
   * 只读策略视图。要求显式 scope+id（不猜测）。
   */
  async readPolicy({ scope, id }, { signal = undefined } = {}) {
    const guard = this._assertNotDisposed()
    if (!guard.ok) return guard
    if (!this.config.baseUrl) {
      return { ok: false, code: "NOT_CONFIGURED", message: "未配置 baseUrl，插件零 I/O；不默认任何端口" }
    }
    if (scope !== "task" && scope !== "project") {
      return { ok: false, code: "BAD_SCOPE", message: "scope 必须为 task|project" }
    }
    if (typeof id !== "string" || id.trim() === "") {
      return { ok: false, code: "BAD_SCOPE_ID", message: `缺少显式 ${scope} id` }
    }
    const qs = scope === "task" ? `task_id=${encodeURIComponent(id)}` : `project=${encodeURIComponent(id)}`
    const url = `${this.config.baseUrl}/api/delivery-policy?${qs}`
    const { signal: callSignal, release } = this._acquireSignal(signal)
    try {
      this._stats.fetches += 1
      const resp = await this.fetcher(url, {
        method: "GET",
        headers: { Accept: "application/json" },
        redirect: "error",
        signal: callSignal,
      })
      if (resp.type === "opaqueredirect") {
        return { ok: false, code: "REDIRECT_REFUSED", message: "服务端返回重定向，已拒绝" }
      }
      if (!resp.ok) {
        const parsed = await safeJson(resp)
        return {
          ok: false,
          code: parsed && typeof parsed.code === "string" ? parsed.code : `HTTP_${resp.status}`,
          message: (parsed && typeof parsed.message === "string" ? parsed.message : `HTTP ${resp.status}`),
          detail: parsed?.detail ?? null,
        }
      }
      const data = await safeJson(resp)
      if (data === undefined) return { ok: false, code: "BAD_JSON", message: "响应 JSON 解析失败" }
      const parsed = parsePolicyView(data)
      if (!parsed.ok) return parsed
      if (parsed.view.scope !== scope || parsed.view.scope_id !== id) {
        return {
          ok: false,
          code: "SCOPE_MISMATCH",
          message: `view 身份与请求不一致（${parsed.view.scope}/${parsed.view.scope_id} vs ${scope}/${id}）`,
        }
      }
      return { ok: true, view: parsed.view, summary: summarizeView(parsed.view) }
    } catch (e) {
      if (callSignal.aborted) {
        return { ok: false, code: "ABORTED", message: "读取已取消（取消/reload/unload/超时）" }
      }
      return { ok: false, code: "NETWORK_UNKNOWN", message: `读取失败：${e?.name ?? "未知"}` }
    } finally {
      release()
    }
  }

  _validateSelection(selection, kind) {
    if (!selection || typeof selection !== "object") {
      return { ok: false, code: "BAD_SELECTION", message: "缺少策略选择" }
    }
    if (selection.inherit === true) {
      if (kind !== "task") {
        return { ok: false, code: "BAD_SELECTION", message: "仅任务可继承项目默认；project 必须显式 policy" }
      }
      if (selection.policy != null) return { ok: false, code: "BAD_SELECTION", message: "inherit不得同时携带policy" }
      return { ok: true }
    }
    const policy = selection.policy
    if (!policy || typeof policy !== "object") {
      return { ok: false, code: "BAD_POLICY", message: "缺少 policy" }
    }
    if (!PRESETS.includes(policy.preset)) {
      return { ok: false, code: "BAD_PRESET", message: `preset 必须为 ${PRESETS.join("/")}` }
    }
    if (!STAGES.includes(policy.stage)) {
      return { ok: false, code: "BAD_STAGE", message: `stage 必须为 ${STAGES.join("/")}` }
    }
    const limits = policy.limits ?? {}
    if (Object.keys(policy).sort().join(",") !== "limits,preset,stage"
        || Object.keys(limits).sort().join(",") !== Object.keys(LIMIT_RANGES).sort().join(",")) {
      return { ok: false, code: "BAD_POLICY", message: "policy/limits不接受未知或缺失字段" }
    }
    for (const [k, [lo, hi]] of Object.entries(LIMIT_RANGES)) {
      const v = limits[k]
      if (!Number.isInteger(v) || v < lo || v > hi) {
        return { ok: false, code: "BAD_LIMITS", message: `limits.${k} 必须为 ${lo}..${hi} 整数` }
      }
    }
    return { ok: true }
  }

  /**
   * 纯函数规划一次写：能力/身份匹配、scope 一致、CAS expected_revision、
   * 精确 confirm（逐字相等、不 trim）、policy/inherit 选择校验。零 I/O。
   * preflight=true 时跳过 confirm 校验（用于确认弹窗前的能力预检）。
   */
  planWrite({ kind, id, view, operationId, reason, selection, confirm = undefined, preflight = false }) {
    if (kind !== "task" && kind !== "project") return { ok: false, code: "BAD_KIND", message: "kind 必须为 task|project" }
    const idField = kind === "task" ? "task_id" : "project"
    if (typeof id !== "string" || id === "") {
      return { ok: false, code: "BAD_ID", message: `缺少显式 ${kind} id` }
    }
    if (id !== id.trim()) {
      return { ok: false, code: "BAD_ID", message: `${idField} 必须为原样身份，不允许前后空白` }
    }
    if (typeof operationId !== "string" || operationId.trim() === "") {
      return { ok: false, code: "BAD_OPERATION_ID", message: "operation_id 必须为非空字符串" }
    }
    if (typeof reason !== "string" || reason.trim() === "") {
      return { ok: false, code: "BAD_REASON", message: "reason 必须为非空字符串" }
    }
    if (!preflight) {
      if (typeof confirm !== "string" || confirm !== id) {
        return {
          ok: false,
          code: "CONFIRM_REQUIRED",
          message: `确认文本必须与 ${idField} 逐字一致（不做 trim）：${id}`,
        }
      }
    }
    if (!view || view.scope !== kind || view.scope_id !== id) {
      return {
        ok: false,
        code: "SCOPE_MISMATCH",
        message: `view 身份与目标不一致（${view?.scope}/${view?.scope_id} vs ${kind}/${id}）`,
      }
    }
    if (!Number.isInteger(view.revision) || view.revision < 0) {
      return { ok: false, code: "BAD_REVISION", message: "view.revision 非法" }
    }
    // 服务端 read_only（GET 视图字段，父级核验）：本地先拒写，不发无谓请求
    if (view.read_only === true) {
      return { ok: false, code: "SERVER_READ_ONLY", message: "服务端处于 read_only 模式，拒绝写请求" }
    }
    const cap = assertWritePreconditions(view, this.config.expected)
    if (!cap.ok) return cap
    const sel = this._validateSelection(selection, kind)
    if (!sel.ok) return sel
    const body = {
      [idField]: id,
      expected_revision: view.revision,
      operation_id: operationId,
      reason,
      confirm: id,
    }
    if (selection.inherit === true) body.inherit = true
    else body.policy = JSON.parse(JSON.stringify(selection.policy))
    const record = {
      kind,
      id,
      operationId,
      reason,
      body,
      fingerprint: fingerprintBody(body),
      target: { scope: kind, scope_id: id },
      baseUrl: this.config.baseUrl,
    }
    if (!preflight) this._confirmedRecords.add(record)
    return { ok: true, record }
  }

  /** 记录“已发送但结果未知”，保留原 operation_id/body/target；禁止自动重投。 */
  _recordUnknown(record, reason) {
    this.ledger.record(record.operationId, {
      status: "unknown",
      unknownReason: reason,
      unknownAt: Date.now(),
      body: record.body,
      kind: record.kind,
      target: record.target,
      reason: record.reason,
      baseUrl: record.baseUrl,
    })
    return {
      ok: false,
      code: "UNKNOWN_OUTCOME",
      message: `已发送但结果未知（${reason}）；原 operation_id/body 已保留，禁止自动重投，可显式复核或转人工。`,
      operationId: record.operationId,
    }
  }

  /**
   * 发送已规划写请求（与 planWrite 产出的 record 严格同体同 ID；不重建 body）。
   * 成功回执展示以 response.current 快照为准，不以动作名断言生效。
   */
  async executeRecord(record, { signal = undefined } = {}) {
    const guard = this._assertNotDisposed()
    if (!guard.ok) return guard
    if (!this.config.baseUrl) {
      return { ok: false, code: "NOT_CONFIGURED", message: "未配置 baseUrl，零 I/O" }
    }
    if (this.config.writesEnabled !== true) {
      return { ok: false, code: "WRITES_DISABLED", message: "默认禁写；需显式配置 writesEnabled=true" }
    }
    if (!record || !this._confirmedRecords.has(record)
        || record.baseUrl !== this.config.baseUrl || record.fingerprint !== fingerprintBody(record.body)
        || record.body?.operation_id !== record.operationId
        || record.body?.[record.kind === "task" ? "task_id" : "project"] !== record.id
        || record.body?.confirm !== record.id) {
      return { ok: false, code: "BAD_RECORD", message: "请求不是本桥逐字确认的原体（预检不能执行），拒绝发送" }
    }
    const begin = this.ledger.begin(record.operationId, record.fingerprint)
    if (!begin.ok) return begin
    const url = `${this.config.baseUrl}/api/delivery-policy/${record.kind}`
    const { signal: callSignal, release } = this._acquireSignal(signal)
    try {
      this._stats.fetches += 1
      this._stats.writesSent += 1
      const resp = await this.fetcher(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-OC-Orch-Web": "1",
          Accept: "application/json",
        },
        body: JSON.stringify(record.body),
        redirect: "error",
        signal: callSignal,
      })
      if (resp.type === "opaqueredirect") {
        return this._recordUnknown(record, "POST重定向结果未知，禁止追随")
      }
      if (!resp.ok) {
        const parsed = await safeJson(resp)
        if (parsed === undefined || resp.status >= 500) {
          // 本地 operator 服务错误均为 JSON；解析不到 -> 结果未知（可能已生效）
          return this._recordUnknown(record, `HTTP ${resp.status}（非 JSON）`)
        }
        // 服务端明确拒绝（含 CAS control_refused）：如实上报，不自动重试
        return {
          ok: false,
          code: typeof parsed.code === "string" ? parsed.code : `HTTP_${resp.status}`,
          message: typeof parsed.message === "string" ? parsed.message : `HTTP ${resp.status}`,
          detail: parsed.detail ?? null,
        }
      }
      const data = await safeJson(resp)
      if (data === undefined) return this._recordUnknown(record, "成功响应非 JSON")
      if (data.ok !== true) {
        return this._recordUnknown(record, "成功HTTP缺少可信业务结果")
      }
      const current = parsePolicyView(data.current)
      if (!current.ok || current.view.scope !== record.kind || current.view.scope_id !== record.id) {
        const unknown = this._recordUnknown(record, "回执current身份/形状未知")
        return { ...unknown, code: "BAD_CURRENT", message: "回执current身份/形状非法；原操作保留待对账" }
      }
      if (data.operation_id !== record.operationId || typeof data.applied !== "boolean"
          || !Number.isSafeInteger(data.applied_revision) || data.applied_revision < 1
          || data.applied_revision > current.view.revision) {
        return this._recordUnknown(record, "回执operation/applied_revision未归属原请求")
      }
      this.ledger.record(record.operationId, {
        status: "applied",
        appliedAt: Date.now(),
        applied: data.applied === true,
        applied_revision: data.applied_revision,
      })
      return {
        ok: true,
        operationId: record.operationId,
        applied: data.applied === true,
        applied_revision: data.applied_revision,
        execution_enforced: current.view.execution_enforced === true,
        current: current.view,
        currentSummary: summarizeView(current.view),
      }
    } catch (e) {
      // 断线/超时/中止：结果未知，保留原体；已发 POST 的取消不算“没执行”
      return this._recordUnknown(record, `网络/中止（${e?.name ?? "未知"}）`)
    } finally {
      release()
    }
  }

  /** 显式复核：用原 operation_id 与原始 body 重发（同 ID 同体幂等；不同体冲突拒绝）。 */
  async replay(operationId, { signal = undefined } = {}) {
    const guard = this._assertNotDisposed()
    if (!guard.ok) return guard
    const entry = this.ledger.get(operationId)
    if (!entry || entry.status !== "unknown" || !entry.body) {
      return { ok: false, code: "NO_UNKNOWN_OP", message: `没有可复核的未知 operation：${operationId}` }
    }
    if (entry.baseUrl !== this.config.baseUrl) {
      return { ok: false, code: "IDENTITY_MISMATCH", message: "原未知操作属于其他服务地址，禁止转投" }
    }
    const latest = await this.readPolicy({scope: entry.kind, id: entry.target?.scope_id}, {signal})
    if (!latest.ok) return latest
    if (latest.view.read_only === true) return {ok: false, code: "SERVER_READ_ONLY", message: "服务端已只读；保留原操作，不重发"}
    const cap = assertWritePreconditions(latest.view, this.config.expected)
    if (!cap.ok) return cap
    const record = {
      kind: entry.kind,
      id: entry.target?.scope_id,
      operationId,
      reason: entry.reason ?? "replay",
      body: entry.body,
      fingerprint: fingerprintBody(entry.body),
      target: entry.target,
      baseUrl: entry.baseUrl,
    }
    this._confirmedRecords.add(record)
    return this.executeRecord(record, { signal })
  }

  /** 查看本桥（共享台账）的未知结果 operation 列表，供 UI 展示/复核选择。 */
  listUnknownOperations() {
    return this.ledger.listUnknown().map((e) => ({
      operationId: e.operationId,
      kind: e.kind,
      target: e.target,
      unknownReason: e.unknownReason,
    }))
  }
}

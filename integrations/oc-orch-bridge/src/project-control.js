/** 显式项目控制薄客户端。只走oc-orch REST，不拦截聊天、不发prompt或RPC。
 * 控制意图先落TUI持久存储，再POST一次；未知结果只GET原回执，绝不自动重投。
 */
import {DeliveryBridge, assertWritePreconditions, fingerprintBody} from "./bridge.js"

export const CONTROL_SCHEMA = "oc-orch.project-control.v1"
export const CONTROL_JOURNAL_KEY = "project-control-intents-v1"
const copy = value => JSON.parse(JSON.stringify(value))
const identity = value => typeof value === "string" && value.length > 0 && value === value.trim()
const integer = (value, min = 0) => Number.isSafeInteger(value) && value >= min
const refusal = (code, message) => ({ok: false, code, message})
const fingerprint = record => fingerprintBody({action: record.action, body: record.body,
  baseUrl: record.baseUrl, releaseId: record.releaseId})
const validEntry = (e, actions) => e && identity(e.operationId) && actions.includes(e.action)
  && e.body?.operation_id === e.operationId && identity(e.body.project) && identity(e.body.session_id)
  && identity(e.body.reason) && e.body.confirm === e.body.project && integer(e.body.expected_revision)
  && integer(e.body.expected_epoch, 1) && identity(e.baseUrl) && identity(e.releaseId)
  && e.fingerprint === fingerprint(e) && ["possible", "acknowledged", "refused", "finished"].includes(e.status)
  && (e.action !== "message" || (typeof e.body.text === "string" && e.body.text.trim()
    && new TextEncoder().encode(e.body.text).length <= 100000))

/** 惰性打开宿主durable store：setup及未配置命令不触及存储。 */
export function createControlJournal(storage, {key = CONTROL_JOURNAL_KEY, actions = ["pause", "resume"]} = {}) {
  let handle
  const open = () => {
    if (!handle) {
      if (typeof storage?.store !== "function") throw new Error("缺少TUI durable storage，拒绝控制写入")
      handle = storage.store(key, {initial: {schema: 1, entries: []}})
    }
    const state = handle[0]
    if (state?.schema !== 1 || !Array.isArray(state.entries)
        || state.entries.some(e => !validEntry(e, actions))) {
      throw new Error("控制意图台账损坏/未来版本，拒绝覆盖")
    }
    return handle
  }
  return {
    list() { return copy(open()[0].entries) },
    async put(entry, {newOnly = false} = {}) {
      if (!validEntry(entry, actions)) throw new Error("非法控制意图")
      const [, update] = open()
      await update(draft => {
        if (draft.schema !== 1 || !Array.isArray(draft.entries)
            || draft.entries.some(e => !validEntry(e, actions))) throw new Error("台账版本/内容变化")
        const existing = draft.entries.find(e => e.operationId === entry.operationId)
        if (existing) {
          if (newOnly) throw new Error("原操作已预留，不得再次POST")
          if (existing.fingerprint !== entry.fingerprint) throw new Error("原操作内容冲突")
          if (existing.status !== "possible" && entry.status === "possible") throw new Error("不能倒退已对账操作")
          Object.assign(existing, copy(entry))
        } else {
          if (entry.action === "message" && draft.entries.some(e => e.action === "message"
              && e.body.project === entry.body.project && e.body.session_id === entry.body.session_id
              && !["finished", "refused"].includes(e.status))) throw new Error("已有原补充指令未结束，先观察，不换ID重投")
          while (draft.entries.length >= 128) {
            const index = draft.entries.findIndex(e => e.action === "message"
              ? ["finished", "refused"].includes(e.status) : e.status !== "possible")
            if (index < 0) throw new Error("未决操作已达128项，先人工对账；不丢旧意图")
            draft.entries.splice(index, 1)
          }
          draft.entries.push(copy(entry))
        }
      })
    },
  }
}

export function parseControlView(data, project, sessionId) {
  const c = data?.project_control, s = data?.session_control, lease = data?.lease
  if (data?.ok !== true || data.schema !== CONTROL_SCHEMA || data.project !== project
      || data.session_id !== sessionId || typeof data.read_only !== "boolean"
      || !["running", "paused"].includes(c?.status) || !integer(c?.revision)
      || typeof s?.held !== "boolean" || typeof s?.user_turn_active !== "boolean"
      || !data.runtime || typeof data.runtime !== "object"
      || (lease !== null && (!identity(lease?.holder_session) || !integer(lease?.epoch, 1)
        || !integer(lease?.expires_at) || ![false, true, 0, 1].includes(lease?.recovery_blocked)))) {
    return refusal("BAD_CONTROL_VIEW", "控制视图身份/类型/覆盖未知，拒绝推断")
  }
  return {ok: true, view: data}
}

async function json(response) {
  try { return await response.json() } catch { return undefined }
}

export class ProjectControlBridge extends DeliveryBridge {
  /** @param {*} config @param {{fetcher?: typeof fetch, journal?: ReturnType<typeof createControlJournal>, now?: () => number}} [options] */
  constructor(config, {fetcher = fetch, journal, now = () => Date.now()} = {}) {
    super(config, {fetcher})
    this.journal = journal
    this.now = now
    this.records = new WeakSet()
  }

  async readControl(project, sessionId, operationId = null) {
    const alive = this._assertNotDisposed()
    if (!alive.ok) return alive
    if (!this.configured) return refusal("NOT_CONFIGURED", "未配置baseUrl，零I/O")
    if (!identity(project) || !identity(sessionId) || (operationId !== null && !identity(operationId))) {
      return refusal("BAD_SCOPE", "项目/session/原操作ID必须显式且原样")
    }
    const query = new URLSearchParams({project, session_id: sessionId})
    if (operationId !== null) query.set("operation_id", operationId)
    const {signal, release} = this._acquireSignal(undefined)
    try {
      const response = await this.fetcher(`${this.config.baseUrl}/api/coordinator/control?${query}`, {
        method: "GET", redirect: "error", headers: {Accept: "application/json"}, signal,
      })
      if (!response.ok || response.type === "opaqueredirect") return refusal("CONTROL_READ_FAILED", "控制读取失败；不当作无暂停/未执行")
      return parseControlView(await json(response), project, sessionId)
    } catch {
      return refusal("CONTROL_READ_FAILED", "控制读取未知；不当作无暂停/未执行")
    } finally { release() }
  }

  preflight(view, project, sessionId) {
    const alive = this._assertNotDisposed()
    if (!alive.ok) return alive
    if (!this.configured) return refusal("NOT_CONFIGURED", "未配置baseUrl，零I/O")
    if (this.config.writesEnabled !== true) return refusal("WRITES_DISABLED", "默认禁写")
    const parsed = parseControlView(view, project, sessionId)
    if (!parsed.ok) return parsed
    // 控制写入必须显式固定真实发布身份；不能通过requireManifest:false放宽。
    if (!identity(this.config.expected?.releaseId) || view.runtime.manifest_verified !== true) {
      return refusal("IDENTITY_REQUIRED", "控制写入需固定releaseId及已核验manifest")
    }
    const cap = assertWritePreconditions(view, this.config.expected)
    if (!cap.ok) return cap
    if (view.read_only) return refusal("SERVER_READ_ONLY", "服务端只读，拒绝控制写入")
    if (view.lease?.holder_session !== sessionId || view.lease.expires_at <= this.now()
        || view.lease.recovery_blocked) return refusal("LEASE_REFUSED", "租约缺失/过期/受阻/holder不符，不自动接管")
    return {ok: true}
  }

  plan({action, project, sessionId, view, operationId, reason, confirm}) {
    const cap = this.preflight(view, project, sessionId)
    if (!cap.ok) return cap
    if (!["pause", "resume"].includes(action) || !identity(operationId) || !identity(reason)) return refusal("BAD_ACTION", "控制动作/操作ID/原因非法")
    if (confirm !== project) return refusal("CONFIRM_REQUIRED", "确认须与项目逐字一致，不trim")
    const body = Object.freeze({project, session_id: sessionId, operation_id: operationId, reason,
      expected_revision: view.project_control.revision, expected_epoch: view.lease.epoch, confirm})
    const record = {action, body, baseUrl: this.config.baseUrl, releaseId: view.runtime.release_id, operationId}
    const frozen = Object.freeze({...record, fingerprint: fingerprint(record)})
    this.records.add(frozen)
    return {ok: true, record: frozen}
  }

  /** @param {*} record @param {{canSend?: () => boolean}} [options] */
  async executeControl(record, {canSend = () => true} = {}) {
    if (!this._assertNotDisposed().ok) return this._assertNotDisposed()
    if (this.config.writesEnabled !== true) return refusal("WRITES_DISABLED", "默认禁写")
    if (!record || !this.records.has(record) || record.baseUrl !== this.config.baseUrl
        || record.releaseId !== this.config.expected?.releaseId || record.fingerprint !== fingerprint(record)) {
      return refusal("BAD_RECORD", "不是本桥逐字确认的不可变原体")
    }
    const entry = {...copy(record), status: "possible"}
    try {
      if (this.journal.list().some(e => e.operationId === record.operationId)) return refusal("ALREADY_REGISTERED", "原操作已登记，只允许GET对账，不重投")
      await this.journal.put(entry, {newOnly: true})
    } catch {
      return refusal("PERSISTENCE_REQUIRED", "控制意图未能持久保存，零POST")
    }
    if (this.disposed || !canSend()) return refusal("UNKNOWN_OUTCOME", "已保留意图但发送已取消；只GET对账，不自动重投")
    const {signal, release} = this._acquireSignal(undefined)
    try {
      const response = await this.fetcher(`${record.baseUrl}/api/coordinator/${record.action}`, {
        method: "POST", redirect: "error", signal,
        headers: {"Content-Type": "application/json", "X-OC-Orch-Web": "1", Accept: "application/json"},
        body: JSON.stringify(record.body),
      })
      const data = await json(response)
      if (response.type !== "opaqueredirect" && response.status >= 400 && response.status < 500
          && data?.ok === false && typeof data.code === "string") {
        await this.journal.put({...entry, status: "refused"})
        return refusal(data.code, data.message ?? "服务端明确拒绝；不自动重试")
      }
      const r = data?.result, snapshot = r?.snapshot
      if (!response.ok || data?.ok !== true || data.executed !== true || r?.executed !== true
          || data.action !== record.action || data.project !== record.body.project
          || data.session_id !== record.body.session_id || r.operation_id !== record.operationId
          || r.project !== record.body.project || r.session_id !== record.body.session_id
          || typeof r.applied !== "boolean" || !integer(r.revision) || r.epoch !== record.body.expected_epoch
          || snapshot?.project !== record.body.project || snapshot.revision !== r.revision
          || !["running", "paused"].includes(snapshot.status)) {
        return refusal("UNKNOWN_OUTCOME", "控制回执未能归属原操作；意图已保存，只GET对账，不重投")
      }
      await this.journal.put({...entry, status: "acknowledged"})
      return {ok: true, operationId: record.operationId, status: snapshot.status, revision: snapshot.revision, applied: r.applied}
    } catch {
      return refusal("UNKNOWN_OUTCOME", "网络/取消/存储结果未知；意图已保存，只GET对账，不自动重投")
    } finally { release() }
  }

  pending() {
    return this.journal.list().filter(e => e.status === "possible" && e.baseUrl === this.config.baseUrl)
  }

  async reconcile(operationId) {
    let entry
    try { entry = this.pending().find(e => e.operationId === operationId) }
    catch { return refusal("PERSISTENCE_REQUIRED", "意图台账不可读，不猜原操作") }
    if (!entry) return refusal("NO_UNKNOWN_OP", "没有此服务的未决操作")
    if (entry.releaseId !== this.config.expected?.releaseId) return refusal("IDENTITY_MISMATCH", "发布身份变化，保留原操作转人工")
    const read = await this.readControl(entry.body.project, entry.body.session_id, operationId)
    if (!read.ok) return read
    const cap = assertWritePreconditions(read.view, this.config.expected)
    if (!cap.ok || read.view.runtime.manifest_verified !== true) return refusal("IDENTITY_MISMATCH", "当前服务身份未核验，保留原操作")
    const receipt = read.view.operation
    if (receipt?.operation_id !== operationId || receipt.action !== entry.action
        || receipt.project !== entry.body.project || receipt.session_id !== entry.body.session_id
        || !integer(receipt.revision_after, 1) || receipt.revision_after > read.view.project_control.revision) {
      return refusal("UNKNOWN_OUTCOME", "尚无可归属的原操作回执；无记录不代表未执行，不重发")
    }
    try { await this.journal.put({...entry, status: "acknowledged"}) }
    catch { return refusal("UNKNOWN_OUTCOME", "原回执已读但对账标记未能保存，保留未决") }
    return {ok: true, operationId, applied_revision: receipt.revision_after,
      status: read.view.project_control.status, revision: read.view.project_control.revision}
  }
}

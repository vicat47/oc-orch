/** 显式补充指令薄客户端：意图先durable，再交oc-orch发送一次；观察不重投模型输入。 */
import {fingerprintBody} from "./bridge.js"
import {ProjectControlBridge} from "./project-control.js"

const refusal = (code, message) => ({ok: false, code, message})
const digest = record => fingerprintBody({action: record.action, body: record.body,
  baseUrl: record.baseUrl, releaseId: record.releaseId})

export class OperatorTurnBridge extends ProjectControlBridge {
  planMessage({project, sessionId, view, operationId, text, confirm}) {
    const cap = this.preflight(view, project, sessionId)
    if (!cap.ok) return cap
    if (typeof text !== "string" || !text.trim() || new TextEncoder().encode(text).length > 100000
        || !Number.isSafeInteger(view.session_control.revision) || view.session_control.revision < 0
        || typeof operationId !== "string" || !operationId || operationId !== operationId.trim()
        || confirm !== project) return refusal("BAD_MESSAGE", "须合法正文、当前session版本和逐字项目确认")
    const body = Object.freeze({project, session_id: sessionId, operation_id: operationId, text,
      reason: "tui:explicit-user-message", expected_revision: view.session_control.revision,
      expected_epoch: view.lease.epoch, confirm})
    const record = {action: "message", body, baseUrl: this.config.baseUrl,
      releaseId: view.runtime.release_id, operationId}
    const result = Object.freeze({...record, fingerprint: digest(record)})
    this.records.add(result)
    return {ok: true, record: result}
  }

  /** @param {*} record @param {{canSend?: () => boolean}} [options] */
  async sendMessage(record, {canSend = () => true} = {}) {
    if (this.disposed || !this.config.writesEnabled || !this.records.has(record)
        || record.baseUrl !== this.config.baseUrl || record.releaseId !== this.config.expected?.releaseId
        || record.fingerprint !== digest(record)) return refusal("BAD_RECORD", "不是当前已确认的原意图")
    const entry = {...record, status: "possible"}
    try { await this.journal.put(entry, {newOnly: true}) }
    catch { return refusal("PERSISTENCE_REQUIRED", "先落持久意图失败或原操作已存在，零POST") }
    if (this.disposed || !canSend()) return refusal("UNKNOWN_OUTCOME", "保留原意图，只对账、不重投")
    const {signal, release} = this._acquireSignal(undefined)
    try {
      const response = await this.fetcher(`${record.baseUrl}/api/coordinator/message`, {method: "POST", redirect: "error", signal,
        headers: {"Content-Type": "application/json", "X-OC-Orch-Web": "1"}, body: JSON.stringify(record.body)})
      const data = await response.json()
      if (response.status >= 400 && response.status < 500 && data?.ok === false && typeof data.code === "string") {
        await this.journal.put({...entry, status: "refused"})
        return refusal(data.code, data.message)
      }
      const op = data?.operation
      if (!response.ok || data?.ok !== true || op?.operation_id !== record.operationId
          || op.project !== record.body.project || op.session_id !== record.body.session_id
          || typeof op.message_id !== "string" || !["admitted", "finished"].includes(op.state)) {
        return refusal("UNKNOWN_OUTCOME", "受理未知，保留原ID；观察不重发消息")
      }
      await this.journal.put({...entry, status: "acknowledged"})
      return {ok: true, operation: op}
    } catch { return refusal("UNKNOWN_OUTCOME", "响应未知，保留原意图，不重新提交") }
    finally { release() }
  }

  async observeMessage(operationId) {
    const record = this.journal.list().find(e => e.operationId === operationId && e.baseUrl === this.config.baseUrl)
    if (!record || this.disposed || record.releaseId !== this.config.expected?.releaseId) return refusal("IDENTITY_MISMATCH", "未找到同地址/发布的原意图")
    const control = await this.readControl(record.body.project, record.body.session_id)
    if (!control.ok) return control
    const cap = this.preflight(control.view, record.body.project, record.body.session_id)
    if (!cap.ok) return cap
    const scope = {project: record.body.project, session_id: record.body.session_id, operation_id: operationId}
    const {signal, release} = this._acquireSignal(undefined)
    try {
      const response = await this.fetcher(`${record.baseUrl}/api/coordinator/message?${new URLSearchParams(scope)}`, {redirect: "error", signal})
      const receipt = await response.json()
      if (!response.ok || receipt?.ok !== true || receipt.operation?.operation_id !== operationId
          || receipt.operation.project !== scope.project || receipt.operation.session_id !== scope.session_id) {
        return refusal("UNKNOWN_OUTCOME", "无可归属回执，不视作未发送，不重投")
      }
      // 这个POST只让oc-orch读原窗口并落结束证据，绝不再次POST模型消息。
      const observed = await this.fetcher(`${record.baseUrl}/api/coordinator/message/observe`, {method: "POST", redirect: "error", signal,
        headers: {"Content-Type": "application/json", "X-OC-Orch-Web": "1"}, body: JSON.stringify(scope)})
      const data = await observed.json()
      if (!observed.ok || data?.ok !== true || data.operation?.operation_id !== operationId
          || data.operation.project !== scope.project || data.operation.session_id !== scope.session_id) return refusal("OBSERVATION_UNKNOWN", "原窗口观察未确认，保持屏障")
      if (data.finished === true && data.operation.state === "finished") await this.journal.put({...record, status: "finished"})
      return {ok: true, operation: data.operation, finished: data.finished === true, reason: data.reason}
    } catch { return refusal("OBSERVATION_UNKNOWN", "只读补证失败；保持原ID/屏障，不重投") }
    finally { release() }
  }
}

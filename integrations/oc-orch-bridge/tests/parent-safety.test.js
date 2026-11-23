// 父独立反例：不以worker fake-host回执代替作用域、未知结果与新鲜能力检查。
import {test} from "node:test"
import assert from "node:assert/strict"
import {DeliveryBridge, normalizeConfig, parsePolicyView, createOperationLedger} from "../src/bridge.js"
import {applyInstructionEntry, INSTRUCTION_KEY} from "../src/instructions.js"
import {createFetch, jsonBody, makeView, makeConfig, routePolicyRead, routePolicyWrite} from "./helpers.js"

const instructionHost = put => ({experimental: {session: {instructions: {entry: {put}}}}})

function ready(ff, options = {}, ledger = createOperationLedger()) {
  const bridge = new DeliveryBridge(normalizeConfig(makeConfig(options)).config, {fetcher: ff.fetch, ledger})
  const planned = bridge.planWrite({kind: "task", id: "T-1", view: makeView(),
    operationId: "parent-op", reason: "explicit", confirm: "T-1", selection: {inherit: true}})
  assert.equal(planned.ok, true)
  return {bridge, record: planned.record, ledger}
}

test("父：真实未纳管view允许version=null，不能把合法历史兼容读成坏状态", () => {
  assert.equal(parsePolicyView(makeView({managed: false, revision: 0, version: null, instruction_target: null})).ok, true)
})

test("父：不完整/坏上限的managed策略不能通过视图schema", () => {
  for (const policy of [{}, {preset: "elastic", stage: "standard", limits: {}},
    {...makeView().version.policy, limits: {...makeView().version.policy.limits, max_parallel: true}}]) {
    assert.equal(parsePolicyView(makeView({version: {policy}})).ok, false)
  }
})

test("父：preflight跳过确认仅能预览，不能直接执行写入", async () => {
  const ff = createFetch()
  routePolicyWrite(ff, "task", () => jsonBody({ok: true, applied: true, applied_revision: 4, current: makeView({revision: 4})}))
  const {bridge} = ready(ff)
  const planned = bridge.planWrite({kind: "task", id: "T-1", view: makeView(), operationId: "preflight",
    reason: "preview", selection: {inherit: true}, preflight: true})
  assert.equal((await bridge.executeRecord(planned.record)).ok, false)
  assert.equal(ff.calls.length, 0)
})

test("父：规划快照不引用可变selection；后续编辑不能偷改已确认原体", () => {
  const ff = createFetch()
  const {bridge} = ready(ff)
  const selected = structuredClone(makeView().version.policy)
  const planned = bridge.planWrite({kind: "task", id: "T-1", view: makeView(), operationId: "detached",
    reason: "explicit", confirm: "T-1", selection: {policy: selected}})
  selected.preset = "latency-first"
  assert.equal(planned.record.body.policy.preset, "budget-first")
})

test("父：HTTP 5xx即使有JSON也可能已提交，必须保原操作为unknown", async () => {
  const ff = createFetch()
  routePolicyWrite(ff, "task", () => jsonBody({ok: false, code: "ACTION_FAILED", message: "post-commit-view-error"}, 500))
  const {bridge} = ready(ff)
  await bridge.executeRecord(readyRecord(bridge))
  assert.equal(bridge.listUnknownOperations().length, 1)
})

function readyRecord(bridge) {
  return bridge.planWrite({kind: "task", id: "T-1", view: makeView(), operationId: "parent-op",
    reason: "explicit", confirm: "T-1", selection: {inherit: true}}).record
}

test("父：成功但别的Task/operation回执不得冒认成功，原请求仍unknown", async () => {
  for (const changes of [{operation_id: "other-op"}, {current: makeView({scope_id: "T-other", revision: 4})}]) {
    const ff = createFetch()
    routePolicyWrite(ff, "task", () => jsonBody({ok: true, applied: true, applied_revision: 4,
      operation_id: "parent-op", current: makeView({revision: 4}), ...changes}))
    const {bridge, record} = ready(ff)
    assert.equal((await bridge.executeRecord(record)).ok, false)
    assert.equal(bridge.listUnknownOperations().length, 1)
  }
})

test("父：POST重定向/坏current均须保原体待对账", async () => {
  for (const response of [{type: "opaqueredirect", ok: false, status: 0},
    jsonBody({ok: true, operation_id: "parent-op", applied: true, applied_revision: 4, current: {}})]) {
    const ff = createFetch()
    routePolicyWrite(ff, "task", () => response)
    const {bridge, record} = ready(ff)
    await bridge.executeRecord(record)
    assert.equal(bridge.listUnknownOperations().length, 1)
  }
})

test("父：原未知请求不能由另一个baseUrl桥接实例转投", async () => {
  const ff = createFetch(), ledger = createOperationLedger()
  routePolicyWrite(ff, "task", () => {throw new TypeError("lost")})
  const {bridge, record} = ready(ff, {}, ledger)
  await bridge.executeRecord(record)
  const other = new DeliveryBridge(normalizeConfig(makeConfig({baseUrl: "http://127.0.0.1:9999"})).config,
    {fetcher: ff.fetch, ledger})
  const count = ff.calls.length
  assert.equal((await other.replay("parent-op")).ok, false)
  assert.equal(ff.calls.length, count)
})

test("父：显式replay也须重读能力/read_only，不得无条件重用旧握手", async () => {
  const ff = createFetch()
  routePolicyWrite(ff, "task", () => {throw new TypeError("lost")})
  routePolicyRead(ff, {view: makeView({read_only: true})})
  const {bridge, record} = ready(ff)
  await bridge.executeRecord(record)
  const posts = ff.count(c => c.method === "POST")
  const result = await bridge.replay("parent-op")
  assert.equal(result.code, "SERVER_READ_ONLY")
  assert.equal(ff.count(c => c.method === "POST"), posts)
})

test("父：instruction固定key不能由调用参数替换为任意条目", async () => {
  const calls = []
  const result = await applyInstructionEntry({host: instructionHost(async v => calls.push(v)),
    view: makeView(), taskId: "T-1", sessionId: "ses-1", explicitUserAction: true, entryKey: "AGENTS.md"})
  assert.equal(result.ok, false)
  assert.deepEqual(calls, [])
  assert.notEqual(INSTRUCTION_KEY, "AGENTS.md")
})

test("父：instruction target还必须匹配view本身的Task身份而非只比请求参数", async () => {
  for (const view of [makeView({scope: "project", scope_id: "proj-a"}), makeView({scope_id: "T-other"})]) {
    const calls = []
    const result = await applyInstructionEntry({host: instructionHost(async v => calls.push(v)),
      view, taskId: "T-1", sessionId: "ses-1", explicitUserAction: true})
    assert.equal(result.ok, false)
    assert.deepEqual(calls, [])
  }
})

test("父：指令同步须服从新读到的目标撤销，不沿用旧成功回执", async () => {
  const calls = []
  const result = await applyInstructionEntry({host: instructionHost(async v => calls.push(v)),
    view: makeView(), taskId: "T-1", sessionId: "ses-1", explicitUserAction: true,
    refreshView: async () => ({ok: true, view: makeView({instruction_target: null})})})
  assert.equal(result.ok, false)
  assert.deepEqual(calls, [])
})

test("父：指令重读期间取消/卸载，不允许晚到PUT", async () => {
  const calls = []
  const result = await applyInstructionEntry({host: instructionHost(async v => calls.push(v)),
    view: makeView(), taskId: "T-1", sessionId: "ses-1", explicitUserAction: true,
    refreshView: async () => ({ok: true, view: makeView()}), canApply: () => false})
  assert.equal(result.code, "DISPOSED")
  assert.deepEqual(calls, [])
})

test("父：确认后的原体被调用方改写，不能带旧指纹POST", async () => {
  const ff = createFetch()
  const {bridge, record} = ready(ff)
  record.body.reason = "changed after confirmation"
  const result = await bridge.executeRecord(record)
  assert.equal(result.code, "BAD_RECORD")
  assert.equal(ff.calls.length, 0)
})

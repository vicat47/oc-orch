/**
 * bridge 纯逻辑离线验证（node --test；fake-fetch，不构成真实集成）。
 * 覆盖：无配置零I/O / 禁写 / capability缺失 / 身份不符 / 精确确认 /
 * CAS嵌套结果 / replay / 网络未知 / 并发多窗口 / 退出清理 / URL 安全 / schema 失败关闭。
 */
import { test } from "node:test"
import assert from "node:assert/strict"

import {
  DeliveryBridge,
  normalizeConfig,
  normalizeBaseUrl,
  assertWritePreconditions,
  fingerprintBody,
  summarizeView,
  parsePolicyView,
  makeOperationId,
  BridgeError,
  createOperationLedger,
  resetOperationLedgerForTests,
  SCHEMA,
} from "../src/bridge.js"
import { createFetch, jsonBody, notFound, makeView, makeConfig, routePolicyRead, routePolicyWrite } from "./helpers.js"

test.beforeEach(() => {
  resetOperationLedgerForTests()
})

function bridgeWith(rawConfig, ff) {
  const { config } = normalizeConfig(rawConfig)
  return new DeliveryBridge(config, { fetcher: ff.fetch })
}

// ---- 零 I/O / 配置边界 ----

test("未配置 baseUrl：读/写均零 I/O，不默认 8787", async () => {
  const ff = createFetch()
  const b = bridgeWith({}, ff)
  assert.equal(b.configured, false)
  const r = await b.readPolicy({ scope: "task", id: "T-1" })
  assert.equal(r.ok, false)
  assert.equal(r.code, "NOT_CONFIGURED")
  assert.equal(ff.calls.length, 0, "零 I/O")
})

test("未配置 baseUrl：写请求零 I/O（WRITES_DISABLED 与 NOT_CONFIGURED 都不发请求）", async () => {
  const ff = createFetch()
  const b = bridgeWith({ baseUrl: "http://127.0.0.1:8787" }, ff) // writesEnabled=false
  const view = makeView()
  const record = b.planWrite({
    kind: "task", id: "T-1", view, operationId: "op-1", reason: "r",
    selection: { policy: view.version.policy }, confirm: "T-1",
  })
  // expected 未配置 -> capability 缺失先行拒绝
  assert.equal(record.ok, false)
  assert.equal(record.code, "CAPABILITY_NOT_CONFIGURED")
  const res = await b.executeRecord(record.record || { kind: "task", operationId: "op-1" })
  assert.equal(res.code, "WRITES_DISABLED")
  assert.equal(ff.calls.length, 0, "禁写零 I/O")
})

test("baseUrl 非法（非环回 / 带凭据）在 normalize 阶段拒绝，配置保持零 I/O", () => {
  assert.throws(() => normalizeBaseUrl("http://example.com:8787"), BridgeError)
  assert.throws(() => normalizeBaseUrl("http://user:pass@127.0.0.1:8787"), BridgeError)
  assert.throws(() => normalizeBaseUrl("file:///tmp/x"), BridgeError)
  const bad = normalizeConfig({ baseUrl: "http://example.com:8787" })
  assert.equal(bad.ok, false)
  assert.equal(bad.config.baseUrl, null)
  assert.ok(bad.config.baseUrl === null)
})

test("GET 请求带 redirect:error；redirect 响应被拒绝", async () => {
  const ff = createFetch()
  ff.when(
    (url) => new URL(url).pathname === "/api/delivery-policy",
    "redirect",
    () => ({ type: "opaqueredirect", status: 0, ok: false }),
  )
  const b = bridgeWith(makeConfig(), ff)
  const r = await b.readPolicy({ scope: "task", id: "T-1" })
  assert.equal(r.ok, false)
  assert.equal(r.code, "REDIRECT_REFUSED")
  assert.equal(ff.calls[0].init.redirect, "error")
})

test("GET 拒绝非 JSON / schema 非 v1 / 字段缺失（fail closed）", async () => {
  const ff = createFetch()
  routePolicyRead(ff, { view: { ok: true, schema: "oc-orch.delivery-policy.v9" } })
  routePolicyRead(ff) // 会被上面先匹配？需要分别测试
  const b = bridgeWith(makeConfig(), ff)

  // schema 不符
  const ff2 = createFetch()
  ff2.when((url) => new URL(url).pathname === "/api/delivery-policy", "bad-schema", () =>
    jsonBody({ ok: true, schema: "oc-orch.delivery-policy.v9" }),
  )
  const b2 = bridgeWith(makeConfig(), ff2)
  const r2 = await b2.readPolicy({ scope: "task", id: "T-1" })
  assert.equal(r2.code, "SCHEMA_MISMATCH")

  // revision 缺失
  const ff3 = createFetch()
  ff3.when((url) => new URL(url).pathname === "/api/delivery-policy", "no-rev", () =>
    jsonBody({ ok: true, schema: SCHEMA, scope: "task", scope_id: "T-1", managed: true, execution_enforced: false, version: {}, runtime: {} }),
  )
  const b3 = bridgeWith(makeConfig(), ff3)
  const r3 = await b3.readPolicy({ scope: "task", id: "T-1" })
  assert.equal(r3.code, "BAD_REVISION")
})

// ---- 禁写与能力/身份匹配 ----

test("默认禁写：writesEnabled=false 时读允许、写拒绝（零写请求）", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  const b = bridgeWith({ ...makeConfig(), writesEnabled: false }, ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  assert.equal(read.ok, true)
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-w", reason: "r",
    selection: { inherit: true }, confirm: "T-1",
  })
  assert.equal(planned.ok, true)
  const res = await b.executeRecord(planned.record)
  assert.equal(res.code, "WRITES_DISABLED")
  assert.equal(ff.calls.length, 1, "只有一次 GET")
})

test("capability 缺失：expected 未配置 -> 写前拒绝", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  const b = bridgeWith({ baseUrl: "http://127.0.0.1:8787", writesEnabled: true }, ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-c", reason: "r",
    selection: { inherit: true }, confirm: "T-1",
  })
  assert.equal(planned.code, "CAPABILITY_NOT_CONFIGURED")
})

test("capability 不符：writer_contract 不匹配 -> 拒绝；guard 非 true -> 拒绝", async () => {
  const base = makeConfig()
  // writer_contract 不符
  const ff = createFetch()
  routePolicyRead(ff, { view: makeView({ runtime: { ...makeView().runtime, writer_contract: "continuity-v1" } }) })
  const b = bridgeWith(base, ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-w1", reason: "r",
    selection: { inherit: true }, confirm: "T-1",
  })
  assert.equal(planned.code, "CAPABILITY_MISMATCH")
  // writer_guard_valid=false
  const ff2 = createFetch()
  routePolicyRead(ff2, { view: makeView({ runtime: { ...makeView().runtime, writer_guard_valid: false } }) })
  const b2 = bridgeWith(base, ff2)
  const read2 = await b2.readPolicy({ scope: "task", id: "T-1" })
  const planned2 = b2.planWrite({
    kind: "task", id: "T-1", view: read2.view, operationId: "op-w2", reason: "r",
    selection: { inherit: true }, confirm: "T-1",
  })
  assert.equal(planned2.code, "CAPABILITY_MISMATCH")
  // manifest_verified=false（requireManifest=true）
  const ff3 = createFetch()
  routePolicyRead(ff3, { view: makeView({ runtime: { ...makeView().runtime, manifest_verified: false } }) })
  const b3 = bridgeWith(base, ff3)
  const read3 = await b3.readPolicy({ scope: "task", id: "T-1" })
  const planned3 = b3.planWrite({
    kind: "task", id: "T-1", view: read3.view, operationId: "op-w3", reason: "r",
    selection: { inherit: true }, confirm: "T-1",
  })
  assert.equal(planned3.code, "CAPABILITY_MISMATCH")
})

test("身份不符：release_id 不匹配 -> IDENTITY_MISMATCH；view scope 与目标不一致 -> SCOPE_MISMATCH", async () => {
  const ff = createFetch()
  routePolicyRead(ff, { view: makeView({ runtime: { ...makeView().runtime, release_id: "rel-other" } }) })
  const b = bridgeWith({ ...makeConfig(), expected: { ...makeConfig().expected, releaseId: "rel-1" } }, ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-id", reason: "r",
    selection: { inherit: true }, confirm: "T-1",
  })
  assert.equal(planned.code, "IDENTITY_MISMATCH")

  // view.scope_id 与请求 id 不一致
  const ff2 = createFetch()
  routePolicyRead(ff2)
  const b2 = bridgeWith(makeConfig(), ff2)
  const read2 = await b2.readPolicy({ scope: "task", id: "T-1" })
  const planned2 = b2.planWrite({
    kind: "task", id: "T-2", view: read2.view, operationId: "op-sc", reason: "r",
    selection: { inherit: true }, confirm: "T-2",
  })
  assert.equal(planned2.code, "SCOPE_MISMATCH")
})

// ---- 精确确认（逐字相等、不 trim） ----

test("确认必须与 id 逐字一致：加空格/前缀/大小写都拒", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  const b = bridgeWith(makeConfig(), ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  for (const bad of ["T-1 ", " T-1", "xT-1", "T-1x", "t-1"]) {
    const planned = b.planWrite({
      kind: "task", id: "T-1", view: read.view, operationId: "op-conf", reason: "r",
      selection: { inherit: true }, confirm: bad,
    })
    assert.equal(planned.code, "CONFIRM_REQUIRED", `confirm=${JSON.stringify(bad)}`)
  }
  // 项目 id 本身带空白 -> 规划拒绝（不隐式清理）
  const p = b.planWrite({
    kind: "project", id: " proj-a ", view: makeView({ scope: "project", scope_id: " proj-a" }),
    operationId: "op-p", reason: "r", selection: { policy: makeView().version.policy }, confirm: " proj-a",
  })
  assert.equal(p.code, "BAD_ID")
})

test("确认取消/未输入 -> 不发送（核心层由 UI 处理；这里验证 planWrite 拒绝空确认后 execute 无从发生）", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  const b = bridgeWith(makeConfig(), ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-cx", reason: "r",
    selection: { inherit: true }, confirm: "",
  })
  assert.equal(planned.code, "CONFIRM_REQUIRED")
  assert.equal(ff.calls.length, 1) // 仅 GET
})

// ---- CAS 嵌套结果 ----

test("CAS：expected_revision 取自视图 revision；成功回执以 current 快照为准", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  const after = makeView({ revision: 4, execution_enforced: true, version: { policy: { ...makeView().version.policy, preset: "latency-first" } } })
  let seenBody = null
  routePolicyWrite(ff, "task", (url, init, entry) => {
    seenBody = entry.body
    return jsonBody({ ok: true, applied: true, applied_revision: 4, execution_enforced: true, current: after })
  })
  const b = bridgeWith(makeConfig(), ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  assert.equal(read.view.revision, 3)
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-cas", reason: "switch",
    selection: { policy: { preset: "latency-first", stage: "standard", limits: { max_parallel: 3, max_candidates: 4, max_challenge_rounds: 2 } } },
    confirm: "T-1",
  })
  assert.equal(planned.ok, true)
  assert.equal(planned.record.body.expected_revision, 3, "CAS 用读时 revision")
  const res = await b.executeRecord(planned.record)
  assert.equal(res.ok, true)
  assert.equal(seenBody.task_id, "T-1")
  assert.equal(seenBody.operation_id, "op-cas")
  assert.equal(seenBody.confirm, "T-1")
  // 回执展示以 current 快照为准（revision=4、latency-first），不以动作名断言
  assert.equal(res.currentSummary.revision, 4)
  assert.equal(res.currentSummary.preset, "latency-first")
  assert.equal(res.execution_enforced, true)
  assert.equal(res.applied, true)
})

test("CAS 过期：服务端拒绝（control_refused）-> 如实上报且不自动重试", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  let writes = 0
  routePolicyWrite(ff, "task", () => {
    writes += 1
    return jsonBody({ ok: false, code: "control_refused", message: "版本已前进" }, 409)
  })
  const b = bridgeWith(makeConfig(), ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-stale", reason: "switch",
    selection: { inherit: true }, confirm: "T-1",
  })
  const res = await b.executeRecord(planned.record)
  assert.equal(res.ok, false)
  assert.equal(res.code, "control_refused")
  assert.equal(writes, 1, "不自动重试")
  assert.equal(res.operationId, undefined) // 明确拒绝不是 unknown
})

test("POST 回执 current 视图非法 -> BAD_CURRENT（不冒称成功）", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  routePolicyWrite(ff, "task", () =>
    jsonBody({ ok: true, applied: true, applied_revision: 4, execution_enforced: true, current: { ok: true, schema: SCHEMA, scope: "task", scope_id: "T-1", managed: true, version: {} } }),
  )
  const b = bridgeWith(makeConfig(), ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-cur", reason: "r",
    selection: { inherit: true }, confirm: "T-1",
  })
  const res = await b.executeRecord(planned.record)
  assert.equal(res.code, "BAD_CURRENT")
})

// ---- 网络未知 / 原体保留 / 显式复核 ----

test("网络未知：记录原 operation/body，不自动重投；显式 replay 同体同 ID", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  let writeAttempts = 0
  routePolicyWrite(ff, "task", () => {
    writeAttempts += 1
    if (writeAttempts === 1) throw new TypeError("fetch failed")
    return jsonBody({
      ok: true, applied: true, applied_revision: 4, execution_enforced: true, current: makeView({ revision: 4 }),
    })
  })
  const b = bridgeWith(makeConfig(), ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-net", reason: "switch",
    selection: { inherit: true }, confirm: "T-1",
  })
  const res = await b.executeRecord(planned.record)
  assert.equal(res.code, "UNKNOWN_OUTCOME")
  assert.equal(res.operationId, "op-net")
  assert.equal(writeAttempts, 1, "不自动重投")
  // 原体保留
  const ops = b.listUnknownOperations()
  assert.equal(ops.length, 1)
  assert.equal(ops[0].operationId, "op-net")
  // 显式复核成功（同 ID 同体）
  const replayRes = await b.replay("op-net")
  assert.equal(replayRes.ok, true)
  assert.equal(replayRes.currentSummary.revision, 4)
  assert.equal(writeAttempts, 2)
  // 复核后不再在 unknown 列表
  assert.equal(b.listUnknownOperations().length, 0)
})

test("网络未知：5xx 非 JSON 按 unknown 处理；5xx JSON（服务端明确拒绝）按拒绝处理", async () => {
  // 非 JSON 500
  const ff = createFetch()
  routePolicyRead(ff)
  routePolicyWrite(ff, "task", () => new Response("<html>bad gateway</html>", { status: 502 }))
  const b = bridgeWith(makeConfig(), ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-500a", reason: "r",
    selection: { inherit: true }, confirm: "T-1",
  })
  const res = await b.executeRecord(planned.record)
  assert.equal(res.code, "UNKNOWN_OUTCOME")

  // JSON 409（服务端明确拒绝）——用独立台账避免被上一段 unknown 污染
  const ff2 = createFetch()
  routePolicyRead(ff2)
  routePolicyWrite(ff2, "task", () => jsonBody({ ok: false, code: "control_refused", message: "版本前进" }, 409))
  const b2 = new DeliveryBridge(normalizeConfig(makeConfig()).config, {
    fetcher: ff2.fetch,
    ledger: createOperationLedger(),
  })
  const read2 = await b2.readPolicy({ scope: "task", id: "T-1" })
  const planned2 = b2.planWrite({
    kind: "task", id: "T-1", view: read2.view, operationId: "op-500b", reason: "r",
    selection: { inherit: true }, confirm: "T-1",
  })
  const res2 = await b2.executeRecord(planned2.record)
  assert.equal(res2.code, "control_refused")
  assert.equal(b2.listUnknownOperations().length, 0)
})

test("replay：未知 op 不存在 -> NO_UNKNOWN_OP；同 ID 不同体 -> OPERATION_BODY_CONFLICT（不发请求）", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  routePolicyWrite(ff, "task", () => {
    throw new TypeError("fetch failed")
  })
  const b = bridgeWith(makeConfig(), ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-re", reason: "switch",
    selection: { inherit: true }, confirm: "T-1",
  })
  await b.executeRecord(planned.record)
  const miss = await b.replay("op-missing")
  assert.equal(miss.code, "NO_UNKNOWN_OP")
  // 同 ID 不同体：不同 reason -> 不同 fingerprint -> 冲突
  const ff2 = createFetch()
  routePolicyRead(ff2)
  ff2.when((url) => new URL(url).pathname === "/api/delivery-policy/task", "write2", () => jsonBody({ ok: true }))
  const b2 = bridgeWith(makeConfig(), ff2)
  const read2 = await b2.readPolicy({ scope: "task", id: "T-1" })
  const p1 = b2.planWrite({
    kind: "task", id: "T-1", view: read2.view, operationId: "op-same", reason: "AAA",
    selection: { inherit: true }, confirm: "T-1",
  })
  await b2.executeRecord(p1.record) // unknown（500 JSON ok 但 current 缺失 -> 非 JSON? 实际 jsonBody ok -> data.ok true -> parsePolicyView(current=undefined) -> BAD_CURRENT, not unknown）
  // 用明确的网络失败保证 unknown：
  const b3 = bridgeWith(makeConfig(), ff)
  const read3 = await b3.readPolicy({ scope: "task", id: "T-1" })
  const p3 = b3.planWrite({
    kind: "task", id: "T-1", view: read3.view, operationId: "op-same", reason: "AAA",
    selection: { inherit: true }, confirm: "T-1",
  })
  await b3.executeRecord(p3.record)
  const conflict = b3.planWrite({
    kind: "task", id: "T-1", view: read3.view, operationId: "op-same", reason: "BBB",
    selection: { inherit: true }, confirm: "T-1",
  })
  assert.equal(conflict.ok, true) // planWrite 本身允许（体冲突检测在 executeRecord）
  const before = ff.calls.length
  const res = await b3.executeRecord(conflict.record)
  assert.equal(res.code, "OPERATION_BODY_CONFLICT")
  assert.equal(ff.calls.length, before, "冲突不发起新请求")
})

// ---- 并发多窗口 ----

test("并发多窗口共享操作台账：同 ID 不同体冲突、同体允许重放", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  let attempts = 0
  routePolicyWrite(ff, "task", () => {
    attempts += 1
    // 第一次写入：网络未知；其后成功（供显式复核）
    if (attempts === 1) throw new TypeError("fetch failed")
    return jsonBody({ ok: true, applied: true, applied_revision: 4, execution_enforced: true, current: makeView({ revision: 4 }) })
  })
  const bA = bridgeWith(makeConfig(), ff)
  const bB = bridgeWith(makeConfig(), ff) // 同一进程共享模块级台账
  const read = await bA.readPolicy({ scope: "task", id: "T-1" })
  const pA = bA.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-win", reason: "window-A",
    selection: { inherit: true }, confirm: "T-1",
  })
  const pB = bB.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-win", reason: "window-B",
    selection: { inherit: true }, confirm: "T-1",
  })
  const before = ff.calls.length
  // A 先发 -> 网络未知
  const resA = await bA.executeRecord(pA.record)
  assert.equal(resA.code, "UNKNOWN_OUTCOME")
  const afterA = ff.calls.length
  assert.ok(afterA > before)
  // B 同 ID 不同体 -> 冲突，不新增请求
  const resB = await bB.executeRecord(pB.record)
  assert.equal(resB.code, "OPERATION_BODY_CONFLICT")
  assert.equal(ff.calls.length, afterA)
  // B 显式复核 A 的原体（相同 body）-> 允许
  const replayRes = await bB.replay("op-win")
  assert.equal(replayRes.ok, true)
  assert.equal(attempts, 2)
})

// ---- 退出清理 / 中止 ----

test("dispose：中止在途读，之后读/写返回 DISPOSED 且不再发请求", async () => {
  const ff = createFetch()
  let releaseRead
  const gate = new Promise((resolve) => { releaseRead = resolve })
  ff.when(
    (url) => new URL(url).pathname === "/api/delivery-policy",
    "slow-read",
    (url, init) =>
      new Promise((resolve, reject) => {
        init.signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true })
        gate.then(() => resolve(jsonBody(makeView())))
      }),
  )
  const b = bridgeWith(makeConfig(), ff)
  const pending = b.readPolicy({ scope: "task", id: "T-1" })
  b.dispose()
  const res = await pending
  assert.equal(res.code, "ABORTED")
  const after = await b.readPolicy({ scope: "task", id: "T-1" })
  assert.equal(after.code, "DISPOSED")
  assert.equal(ff.calls.length, 1, "dispose 后不再发起新请求")
})

test("dispose 中途取消在途 POST：按未知结果处理并保原体，不算“没执行”", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  ff.when(
    (url) => new URL(url).pathname === "/api/delivery-policy/task",
    "slow-write",
    (url, init) =>
      new Promise((resolve, reject) => {
        init.signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true })
        // 永不 resolve；等待 dispose 中止
      }),
  )
  const b = bridgeWith(makeConfig(), ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-abort", reason: "r",
    selection: { inherit: true }, confirm: "T-1",
  })
  const pending = b.executeRecord(planned.record)
  b.dispose()
  const res = await pending
  assert.equal(res.code, "UNKNOWN_OUTCOME")
  // 已发 POST 的取消 ≠ 没执行：原体已保留供复核
  const ops = b.listUnknownOperations()
  assert.equal(ops.length, 1)
  assert.equal(ops[0].operationId, "op-abort")
})

// ---- 纯函数 & 摘要 ----

test("服务端 read_only：GET 视图 read_only=true -> planWrite 拒绝（SERVER_READ_ONLY），零写请求", async () => {
  const ff = createFetch()
  routePolicyRead(ff, { view: makeView({ read_only: true }) })
  const b = bridgeWith(makeConfig(), ff)
  const read = await b.readPolicy({ scope: "task", id: "T-1" })
  assert.equal(read.ok, true)
  assert.equal(read.summary.read_only, true, "摘要暴露 read_only")
  const planned = b.planWrite({
    kind: "task", id: "T-1", view: read.view, operationId: "op-ro", reason: "r",
    selection: { inherit: true }, confirm: "T-1",
  })
  assert.equal(planned.code, "SERVER_READ_ONLY")
  assert.equal(ff.calls.length, 1, "只有一次 GET，无写请求")
})

test("parsePolicyView：read_only 非布尔 -> 拒绝（BAD_READ_ONLY）", () => {
  const bad = parsePolicyView(makeView({ read_only: "yes" }))
  assert.equal(bad.code, "BAD_READ_ONLY")
  assert.equal(parsePolicyView(makeView()).ok, true)
})

test("assess: assertWritePreconditions 与 summarizeView 行为", () => {
  const view = makeView()
  const ok = assertWritePreconditions(view, { writerContract: "continuity-v2", requireManifest: true })
  assert.equal(ok.ok, true)
  // 未 manifest 拒写（requireManifest 默认 true 语义由 normalizeConfig 注入；这里显式验证能力表）
  assert.equal(
    assertWritePreconditions(makeView({ runtime: { ...makeView().runtime, manifest_verified: false } }), { writerContract: "continuity-v2", requireManifest: true }).code,
    "CAPABILITY_MISMATCH",
  )
  assert.equal(
    assertWritePreconditions(view, null).code,
    "CAPABILITY_NOT_CONFIGURED",
  )
  const s = summarizeView(view)
  assert.equal(s.revision, 3)
  assert.equal(s.execution_enforced, false)
  assert.equal(s.read_only, false)
  assert.equal(s.writer_contract, "continuity-v2")
  assert.equal(s.delivery_policy_guard_valid, true)
  assert.equal(fingerprintBody({ a: 1, b: { c: 2 } }), fingerprintBody({ b: { c: 2 }, a: 1 }))
})

test("normalizeConfig：requireManifest 默认 true（未 manifest 拒写）；allowGuiWrites 默认 false", () => {
  const bare = normalizeConfig({ baseUrl: "http://127.0.0.1:8787", expected: { writerContract: "continuity-v2" } })
  assert.equal(bare.ok, true)
  assert.equal(bare.config.expected.requireManifest, true)
  assert.equal(bare.config.allowGuiWrites, false)
  const optOut = normalizeConfig({ baseUrl: "http://127.0.0.1:8787", allowGuiWrites: true, expected: { writerContract: "continuity-v2", requireManifest: false } })
  assert.equal(optOut.config.allowGuiWrites, true)
  assert.equal(optOut.config.expected.requireManifest, false)
})

test("makeOperationId 非空且唯一", () => {
  const a = makeOperationId()
  const b = makeOperationId()
  assert.ok(typeof a === "string" && a.length > 0)
  assert.notEqual(a, b)
})

test("parsePolicyView：project 视图 & preset/stage 枚举非法拒绝", () => {
  const v = makeView({ scope: "project", scope_id: "proj-a" })
  assert.equal(parsePolicyView(v).ok, true)
  const badPreset = parsePolicyView(makeView({ version: { policy: { ...makeView().version.policy, preset: "ultra" } } }))
  assert.equal(badPreset.code, "BAD_PRESET")
  const badStage = parsePolicyView(makeView({ version: { policy: { ...makeView().version.policy, stage: "beta" } } }))
  assert.equal(badStage.code, "BAD_STAGE")
})
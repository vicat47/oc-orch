/**
 * TUI 命令核心离线验证（fake context / fake-fetch / fake host；不构成真实集成）。
 * 覆盖：加载零 I/O / 零 client 调用 / 未配置零 I/O / 默认禁写 /
 * 精确确认（不 trim）/ 取消未发送 / CAS 与能力拒绝 / 成功按 current 快照展示 /
 * 动态指令仅显式动作后且 scope 匹配 / 不猜 sessionID / 退出清理忽略晚到 UI / replay。
 */
import { test } from "node:test"
import assert from "node:assert/strict"

import { makeTuiPlugin } from "../src/tui-core.js"
import { INSTRUCTION_KEY } from "../src/instructions.js"
import { createFetch, jsonBody, makeView, makeConfig, routePolicyRead, routePolicyWrite } from "./helpers.js"
import { getSharedLedger, resetOperationLedgerForTests } from "../src/bridge.js"

test.beforeEach(() => {
  resetOperationLedgerForTests()
})

function fakeHost() {
  const calls = { put: [], remove: [] }
  return {
    calls,
    experimental: { session: {
      instructions: {
        entry: {
          put: async (input) => calls.put.push(input),
          remove: async (input) => calls.remove.push(input),
        },
      },
    } },
  }
}

/**
 * 最小 TUI context。dialogScript: { alert?:[], confirm?:[], prompt?:[], select?:[] }
 * 每次调用消费一个值；默认值 alert->undefined / confirm->true / prompt->"" / select->undefined。
 * 注意：layer 工厂在 makeTuiPlugin 之后用 commandsOf(层工厂) 求值（对齐宿主一次性求值）。
 */
function buildTuiContext({ options = {}, dialogs = {}, sessionList = () => [], client = null } = {}) {
  const queues = {
    alert: [...(dialogs.alert ?? [])],
    confirm: [...(dialogs.confirm ?? [])],
    prompt: [...(dialogs.prompt ?? [])],
    select: [...(dialogs.select ?? [])],
  }
  const toasts = []
  const layerFactories = []
  const ctx = {
    options,
    keymap: {
      layer: (factoryFn) => {
        layerFactories.push(factoryFn)
        return () => {}
      },
    },
    ui: {
      dialog: {
        alert: async (p) => {
          const handler = queues.alert.shift()
          return typeof handler === "function" ? handler(p) : undefined
        },
        confirm: async () => (queues.confirm.length ? queues.confirm.shift() : true),
        prompt: async () => (queues.prompt.length ? queues.prompt.shift() : ""),
        select: async () => (queues.select.length ? queues.select.shift() : undefined),
      },
      toast: { show: (t) => toasts.push(t) },
    },
    data: { session: { list: sessionList } },
    client,
  }
  return {
    ctx,
    toasts,
    layerFactories,
    /** 测试中向对话框队列追加脚本值（如 replay 选取真实 operationId）。 */
    pushDialog(kind, value) {
      queues[kind].push(value)
    },
  }
}

function commandsOf(layerFactories) {
  const commands = []
  for (const factoryFn of layerFactories) {
    const layer = factoryFn()
    if (layer && Array.isArray(layer.commands)) commands.push(...layer.commands)
  }
  return commands
}

function cmd(commands, id) {
  const c = commands.find((x) => x.id === id)
  assert.ok(c, `命令未注册：${id}`)
  return c
}

const CONFIG = makeConfig()

// ---- 加载边界 ----

test("加载：零 I/O、零 client 调用，仅注册命令；返回清理函数", () => {
  const ff = createFetch()
  const throwing = new Proxy({}, { get: () => { throw new Error("不应在加载时访问 client") } })
  const { ctx, layerFactories, toasts } = buildTuiContext({
    options: { ...CONFIG, baseUrl: "http://127.0.0.1:8787" },
    client: throwing,
  })
  const cleanup = makeTuiPlugin(ctx, { fetcher: ff.fetch })
  assert.equal(typeof cleanup, "function")
  assert.equal(ff.calls.length, 0, "加载零 I/O")
  const commands = commandsOf(layerFactories)
  const ids = commands.map((c) => c.id)
  assert.ok(ids.includes("oc-orch.delivery-policy.status"))
  assert.ok(ids.includes("oc-orch.delivery-policy.switch-task"))
  assert.ok(ids.includes("oc-orch.delivery-policy.switch-project"))
  assert.ok(ids.includes("oc-orch.delivery-policy.replay"))
  assert.ok(ids.includes("oc-orch.delivery-policy.sync-instructions"))
  assert.equal(toasts.length, 0)
})

test("未配置：status 命令零 I/O 且提示", async () => {
  const ff = createFetch()
  const { ctx, layerFactories, toasts } = buildTuiContext({ options: {} })
  makeTuiPlugin(ctx, { fetcher: ff.fetch })
  const commands = commandsOf(layerFactories)
  await cmd(commands, "oc-orch.delivery-policy.status").run()
  assert.equal(ff.calls.length, 0)
  assert.ok(toasts.some((t) => t.message.includes("零 I/O")))
})

// ---- 状态 ----

test("status：读取当前视图并用 dialog.alert 展示摘要", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  const alerts = []
  const { ctx, layerFactories, toasts } = buildTuiContext({
    options: CONFIG,
    dialogs: { alert: [(p) => { alerts.push(p); return p }] },
  })
  makeTuiPlugin(ctx, { fetcher: ff.fetch })
  const commands = commandsOf(layerFactories)
  await cmd(commands, "oc-orch.delivery-policy.status").run()
  assert.equal(ff.calls.length, 1)
  assert.equal(alerts.length, 1)
  assert.match(alerts[0].message, /revision=r3/)
  assert.match(alerts[0].message, /budget-first/)
  assert.match(alerts[0].message, /草案，未生效/)
  assert.equal(toasts.length, 0)
})

// ---- 切换（写）----

test("switch-task 成功：GET+POST、CAS expected_revision、确认逐字、按 current 快照展示、指令同步写固定 key", async () => {
  const ff = createFetch()
  let currentView = makeView()
  ff.when((url, init) => (init.method ?? "GET") === "GET" && new URL(url).pathname === "/api/delivery-policy",
    "fresh-current", () => jsonBody(currentView))
  let posted = null
  const after = makeView({ revision: 4, execution_enforced: true })
  routePolicyWrite(ff, "task", (url, init, entry) => {
    posted = entry.body
    currentView = after
    return jsonBody({ ok: true, applied: true, applied_revision: 4, execution_enforced: true, current: after })
  })
  const host = fakeHost()
  const { ctx, layerFactories, toasts } = buildTuiContext({
    options: CONFIG,
    client: host,
    dialogs: {
      select: ["budget-first", "prototype"],
      prompt: ["2,4,1", "T-1"],
      confirm: [true],
    },
  })
  makeTuiPlugin(ctx, { fetcher: ff.fetch })
  const commands = commandsOf(layerFactories)
  await cmd(commands, "oc-orch.delivery-policy.switch-task").run()
  assert.ok(posted, "POST 已发送")
  assert.equal(posted.task_id, "T-1")
  assert.equal(posted.expected_revision, 3)
  assert.equal(posted.operation_id.length > 0, true)
  assert.equal(posted.confirm, "T-1")
  const okToast = toasts.find((t) => t.message.includes("切换结果"))
  assert.match(okToast.message, /revision=r4/)
  assert.match(okToast.message, /preset=budget-first/)
  // 回执执行约束：after 视图 execution_enforced=true，故不显示“草案”
  assert.doesNotMatch(okToast.message, /草案/)
  // POST 请求头包含 server 写契约
  const postCall = ff.calls.find((c) => c.method === "POST")
  assert.equal(postCall.init.headers["X-OC-Orch-Web"], "1")
  // 指令同步：固定 Task 作用域 key，写入 server 声明且匹配的 session
  assert.equal(host.calls.put.length, 1)
  assert.equal(host.calls.put[0].sessionID, "ses-1")
  assert.equal(host.calls.put[0].key, INSTRUCTION_KEY)
  assert.match(host.calls.put[0].value, /已生效/)
})

test("switch-project 成功：POST /api/delivery-policy/project，confirm=project，无需 inherit", async () => {
  const ff = createFetch()
  const projectView = makeView({ scope: "project", scope_id: "proj-a", project: "proj-a", instruction_target: undefined })
  ff.when(
    (url) => {
      const u = new URL(url)
      return u.pathname === "/api/delivery-policy" && u.searchParams.has("project")
    },
    "policy-read-project",
    () => jsonBody(projectView),
  )
  let posted = null
  routePolicyWrite(ff, "project", (url, init, entry) => {
    posted = entry.body
    return jsonBody({
      ok: true,
      applied: true,
      applied_revision: 4,
      execution_enforced: false,
      current: {
        ...projectView,
        revision: 4,
        version: { policy: { preset: "elastic", stage: "standard", limits: { max_parallel: 3, max_candidates: 4, max_challenge_rounds: 2 } } },
      },
    })
  })
  const { ctx, layerFactories, toasts } = buildTuiContext({
    options: { ...makeConfig(), taskScope: null },
    dialogs: { select: ["elastic", "standard"], prompt: ["3,4,2", "proj-a"] },
  })
  makeTuiPlugin(ctx, { fetcher: ff.fetch })
  const commands = commandsOf(layerFactories)
  await cmd(commands, "oc-orch.delivery-policy.switch-project").run()
  assert.ok(posted, "POST 已发送")
  assert.equal(posted.project, "proj-a")
  assert.equal(posted.expected_revision, 3)
  assert.equal(posted.confirm, "proj-a")
  assert.equal(posted.inherit, undefined, "项目切换必须显式 policy")
  assert.equal(posted.policy.preset, "elastic")
  const resToast = toasts.find((t) => t.message.includes("切换结果"))
  assert.match(resToast.message, /revision=r4/)
  assert.match(resToast.message, /elastic/)
  assert.match(resToast.message, /草案，未生效/)
})

test("switch-task：确认文本带尾随空格 -> CONFIRM_REQUIRED，POST 未发送", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  let posted = false
  routePolicyWrite(ff, "task", () => { posted = true; return jsonBody({ ok: true }) })
  const { ctx, layerFactories, toasts } = buildTuiContext({
    options: CONFIG,
    dialogs: { select: ["budget-first", "prototype"], prompt: ["2,4,1", "T-1 "] },
  })
  makeTuiPlugin(ctx, { fetcher: ff.fetch })
  const commands = commandsOf(layerFactories)
  await cmd(commands, "oc-orch.delivery-policy.switch-task").run()
  assert.equal(posted, false, "确认不符不发送")
  assert.ok(toasts.some((t) => t.message.includes("CONFIRM_REQUIRED")))
  assert.equal(ff.count((c) => c.method === "POST"), 0)
})

test("switch-task：确认取消（空输入）-> 未发送", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  let posted = false
  routePolicyWrite(ff, "task", () => { posted = true; return jsonBody({ ok: true }) })
  const { ctx, layerFactories, toasts } = buildTuiContext({
    options: CONFIG,
    dialogs: { select: ["budget-first", "prototype"], prompt: ["2,4,1", ""] },
  })
  makeTuiPlugin(ctx, { fetcher: ff.fetch })
  const commands = commandsOf(layerFactories)
  await cmd(commands, "oc-orch.delivery-policy.switch-task").run()
  assert.equal(posted, false)
  assert.ok(toasts.some((t) => t.message.includes("已取消确认")))
  assert.equal(ff.count((c) => c.method === "POST"), 0)
})

test("switch：默认禁写 -> 零请求", async () => {
  const ff = createFetch()
  const { ctx, layerFactories, toasts } = buildTuiContext({
    options: { ...CONFIG, writesEnabled: false },
  })
  makeTuiPlugin(ctx, { fetcher: ff.fetch })
  const commands = commandsOf(layerFactories)
  await cmd(commands, "oc-orch.delivery-policy.switch-task").run()
  assert.equal(ff.calls.length, 0)
  assert.ok(toasts.some((t) => t.message.includes("默认禁写")))
})

test("switch：capability 不符 -> 拒绝，且不进入确认、不发 POST", async () => {
  const ff = createFetch()
  routePolicyRead(ff, {
    view: makeView({ runtime: { ...makeView().runtime, writer_contract: "continuity-v1" } }),
  })
  let posted = false
  routePolicyWrite(ff, "task", () => { posted = true; return jsonBody({ ok: true }) })
  const { ctx, layerFactories, toasts } = buildTuiContext({ options: CONFIG })
  makeTuiPlugin(ctx, { fetcher: ff.fetch })
  const commands = commandsOf(layerFactories)
  await cmd(commands, "oc-orch.delivery-policy.switch-task").run()
  assert.equal(posted, false)
  assert.ok(toasts.some((t) => t.message.includes("CAPABILITY_MISMATCH")))
})

test("switch 网络未知：toast 提示已保存原 operation，可显式复核；不自动重投", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  let attempts = 0
  routePolicyWrite(ff, "task", () => {
    attempts += 1
    throw new TypeError("fetch failed")
  })
  const { ctx, layerFactories, toasts } = buildTuiContext({
    options: CONFIG,
    client: fakeHost(),
    dialogs: { select: ["budget-first", "prototype"], prompt: ["2,4,1", "T-1"] },
  })
  makeTuiPlugin(ctx, { fetcher: ff.fetch })
  const commands = commandsOf(layerFactories)
  await cmd(commands, "oc-orch.delivery-policy.switch-task").run()
  assert.equal(attempts, 1, "不自动重投")
  assert.ok(toasts.some((t) => t.message.includes("UNKNOWN_OUTCOME")))
  assert.ok(toasts.some((t) => t.message.includes("禁止自动重投")))
})

test("replay 命令：列出未知 operation，显式原体重发", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  const written = []
  routePolicyWrite(ff, "task", () => {
    if (written.length === 0) {
      written.push("first")
      // 首次写入：结果未知（成功状态但响应非 JSON）
      return new Response("ok", { status: 200 })
    }
    written.push("replay")
    return jsonBody({ ok: true, applied: true, applied_revision: 4, execution_enforced: true, current: makeView({ revision: 4 }) })
  })
  const { ctx, layerFactories, toasts, pushDialog } = buildTuiContext({
    options: CONFIG,
    client: fakeHost(),
    dialogs: { select: ["budget-first", "prototype"], prompt: ["2,4,1", "T-1"] },
  })
  makeTuiPlugin(ctx, { fetcher: ff.fetch })
  const commands = commandsOf(layerFactories)
  await cmd(commands, "oc-orch.delivery-policy.switch-task").run()
  assert.equal(written.length, 1, "首次写入结果未知")
  assert.ok(toasts.some((t) => t.message.includes("UNKNOWN_OUTCOME")))
  // 从共享台账取被保存的未知 operation，显式复核（同 ID 同体）
  const unknowns = getSharedLedger().listUnknown()
  assert.equal(unknowns.length, 1)
  pushDialog("select", unknowns[0].operationId)
  await cmd(commands, "oc-orch.delivery-policy.replay").run()
  assert.deepEqual(written, ["first", "replay"], "仅显式复核触发重发")
  assert.ok(toasts.some((t) => t.message.includes("复核成功")))
})

test("sync-instructions：无成功切换 -> 提示且不写", async () => {
  const host = fakeHost()
  const { ctx, layerFactories, toasts } = buildTuiContext({ options: CONFIG, client: host })
  makeTuiPlugin(ctx, { fetcher: createFetch().fetch })
  const commands = commandsOf(layerFactories)
  await cmd(commands, "oc-orch.delivery-policy.sync-instructions").run()
  assert.equal(host.calls.put.length, 0)
  assert.ok(toasts.some((t) => t.message.includes("尚无成功切换")))
})

test("不猜 sessionID：未配置 sessionID 且列表为空 -> 显式 prompt 输入；与 target 匹配才写", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  routePolicyWrite(ff, "task", () =>
    jsonBody({ ok: true, applied: true, applied_revision: 4, execution_enforced: true, current: makeView({ revision: 4 }) }),
  )
  const host = fakeHost()
  const { ctx, layerFactories, toasts } = buildTuiContext({
    options: { ...CONFIG, sessionID: null },
    client: host,
    sessionList: () => [],
    dialogs: {
      select: ["budget-first", "prototype"],
      prompt: ["2,4,1", "T-1", "ses-1"],
      confirm: [true],
    },
  })
  const cleanup = makeTuiPlugin(ctx, { fetcher: ff.fetch })
  const commands = commandsOf(layerFactories)
  await cmd(commands, "oc-orch.delivery-policy.switch-task").run()
  assert.equal(host.calls.put.length, 1)
  assert.equal(host.calls.put[0].sessionID, "ses-1")
  cleanup()
})

// ---- 退出清理 / 晚到 UI ----

test("unload/reload：清理后晚到结果被忽略（不再产生 toast）", async () => {
  const ff = createFetch()
  let release
  const gate = new Promise((resolve) => { release = resolve })
  ff.when(
    (url) => new URL(url).pathname === "/api/delivery-policy",
    "slow-read",
    (url, init) =>
      new Promise((resolvePromise, reject) => {
        init.signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true })
        gate.then(() => resolvePromise(jsonBody(makeView())))
      }),
  )
  const { ctx, layerFactories, toasts } = buildTuiContext({ options: CONFIG })
  const cleanup = makeTuiPlugin(ctx, { fetcher: ff.fetch })
  const commands = commandsOf(layerFactories)
  const pending = cmd(commands, "oc-orch.delivery-policy.status").run()
  cleanup()
  release()
  await pending
  assert.equal(ff.calls.length, 1)
  assert.equal(toasts.length, 0, "晚到结果被忽略")
})

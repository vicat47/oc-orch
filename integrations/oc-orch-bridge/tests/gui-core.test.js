/**
 * GUI 命令核心离线验证（fake ctx / fake-fetch；纯 JS 无 JSX）。
 * 覆盖：加载零 I/O 与命令注册 / 未配置零 I/O / 默认禁写 / 精确确认 /
 * 不猜 sessionID / 动态指令 scope 隔离 / 退出清理。
 */
import { test } from "node:test"
import assert from "node:assert/strict"

import { makeGuiPlugin } from "../src/gui-core.js"
import { INSTRUCTION_KEY } from "../src/instructions.js"
import { createFetch, jsonBody, makeView, makeConfig, routePolicyRead, routePolicyWrite } from "./helpers.js"
import { resetOperationLedgerForTests } from "../src/bridge.js"

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

function buildGuiContext({ options = {}, dialogs = {}, client = null, fetcher } = {}) {
  const queues = {
    confirm: [...(dialogs.confirm ?? [])],
    prompt: [...(dialogs.prompt ?? [])],
    select: [...(dialogs.select ?? [])],
  }
  const toasts = []
  const registered = []
  const Command = Symbol("Command")
  const ctx = {
    options,
    add: (registry, fn) => {
      assert.equal(registry, Command)
      registered.push(fn())
      return () => {}
    },
    dialogs: {
      confirm: async (p) => (queues.confirm.length ? queues.confirm.shift() : true),
      prompt: async (p) => (queues.prompt.length ? queues.prompt.shift() : ""),
      select: async (p) => (queues.select.length ? queues.select.shift() : undefined),
    },
    toast: { show: (t) => toasts.push(t) },
    client,
  }
  const cleanup = makeGuiPlugin(ctx, { Command, fetcher })
  return { ctx, cleanup, toasts, registered }
}

const CONFIG = makeConfig()

test("GUI 加载：零 I/O、零 client 调用，注册 5 个命令；返回清理函数", () => {
  const ff = createFetch()
  const throwing = new Proxy({}, { get: () => { throw new Error("不应在加载时访问 client") } })
  const { cleanup, registered } = buildGuiContext({ options: CONFIG, client: throwing, fetcher: ff.fetch })
  assert.equal(typeof cleanup, "function")
  assert.equal(ff.calls.length, 0)
  const ids = registered.map((c) => c.id).sort()
  assert.deepEqual(ids, ["replay", "status", "switch-project", "switch-task", "sync-instructions"].sort())
  assert.ok(registered.every((c) => !Object.hasOwn(c, "bind")), "不绑定任何按键（无 Enter 替换）")
})

test("GUI 未配置：status 零 I/O", async () => {
  const ff = createFetch()
  const { registered, toasts } = buildGuiContext({ options: {}, fetcher: ff.fetch })
  await registered.find((c) => c.id === "status").run()
  assert.equal(ff.calls.length, 0)
  assert.ok(toasts.some((t) => t.description.includes("零 I/O")))
})

test("GUI status：展示策略快照", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  const { registered, toasts } = buildGuiContext({ options: CONFIG, fetcher: ff.fetch })
  await registered.find((c) => c.id === "status").run()
  assert.equal(ff.calls.length, 1)
  const t = toasts.find((x) => x.variant === "default")
  assert.match(t.description, /revision=r3/)
  assert.match(t.description, /草案，未生效/)
})

test("GUI switch-task 成功：POST 与指令同步（固定 key、scope 匹配）", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  let posted = null
  routePolicyWrite(ff, "task", (url, init, entry) => {
    posted = entry.body
    return jsonBody({ ok: true, applied: true, applied_revision: 4, execution_enforced: false, current: makeView({ revision: 4 }) })
  })
  const host = fakeHost()
  const { registered, toasts } = buildGuiContext({
    options: { ...CONFIG, allowGuiWrites: true },
    client: host,
    fetcher: ff.fetch,
    dialogs: { select: ["budget-first", "prototype"], prompt: ["2,4,1", "T-1"], confirm: [true] },
  })
  await registered.find((c) => c.id === "switch-task").run()
  assert.equal(posted.task_id, "T-1")
  assert.equal(posted.expected_revision, 3)
  assert.equal(posted.confirm, "T-1")
  assert.equal(host.calls.put.length, 1)
  assert.equal(host.calls.put[0].key, INSTRUCTION_KEY)
  assert.equal(host.calls.put[0].sessionID, "ses-1")
  const resToast = toasts.find((t) => t.title === "切换结果")
  assert.match(resToast.description, /草案，未生效/)
})

test("GUI switch-task：确认带尾随空格 -> 拒绝，POST 未发送", async () => {
  const ff = createFetch()
  routePolicyRead(ff)
  let posted = false
  routePolicyWrite(ff, "task", () => { posted = true; return jsonBody({ ok: true }) })
  const { registered, toasts } = buildGuiContext({
    options: { ...CONFIG, allowGuiWrites: true },
    fetcher: ff.fetch,
    dialogs: { select: ["budget-first", "prototype"], prompt: ["2,4,1", "T-1 "] },
  })
  await registered.find((c) => c.id === "switch-task").run()
  assert.equal(posted, false)
  assert.ok(toasts.some((t) => t.title === "拒绝"))
  assert.equal(ff.count((c) => c.method === "POST"), 0)
})

test("GUI 候选默认只读：allowGuiWrites 未启用 -> 写命令零 I/O 且提示（跨源通道未核验）", async () => {
  const ff = createFetch()
  const { registered, toasts } = buildGuiContext({ options: CONFIG, fetcher: ff.fetch })
  await registered.find((c) => c.id === "switch-task").run()
  await registered.find((c) => c.id === "switch-project").run()
  await registered.find((c) => c.id === "replay").run()
  assert.equal(ff.calls.length, 0, "写命令零 I/O")
  assert.ok(toasts.some((t) => t.title === "GUI 候选只读"))
  assert.ok(ff.count(() => true) === 0)
})

test("GUI status：跨源/网络失败明确提示（fake-fetch 不代表真实跨源可用）", async () => {
  const ff = createFetch()
  const { registered, toasts } = buildGuiContext({ options: CONFIG, fetcher: ff.fetch })
  await registered.find((c) => c.id === "status").run()
  // 未匹配路由 -> 抛 TypeError -> NETWORK_UNKNOWN；提示跨源/预检限制
  const t = toasts.find((x) => x.title.includes("读取失败"))
  assert.ok(t, "应有读取失败 toast")
  assert.match(t.description, /跨源|网络/)
})

test("GUI 默认禁写：不发请求", async () => {
  const ff = createFetch()
  const { registered, toasts } = buildGuiContext({ options: { ...CONFIG, writesEnabled: false }, fetcher: ff.fetch })
  await registered.find((c) => c.id === "switch-task").run()
  assert.equal(ff.calls.length, 0)
  assert.ok(toasts.some((t) => t.title === "默认禁写"))
})

test("GUI 清理：dispose 后桥失效，不再发请求；晚到 UI 忽略", async () => {
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
  const { registered, toasts, cleanup } = buildGuiContext({ options: CONFIG, fetcher: ff.fetch })
  const pending = registered.find((c) => c.id === "status").run()
  cleanup()
  release()
  await pending
  assert.equal(toasts.length, 0, "清理后晚到结果不更新 UI")
})

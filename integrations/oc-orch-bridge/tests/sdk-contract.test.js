// 2.2.5真实声明核验发现的边界；仍是离线反例，不冒充宿主加载。
import {test} from "node:test"
import assert from "node:assert/strict"
import {makeGuiPlugin} from "../src/gui-core.js"
import {applyInstructionEntry, removeInstructionEntry} from "../src/instructions.js"
import {makeView} from "./helpers.js"

test("SDK：指令只走experimental正式路径，不读取已弃用session别名", async () => {
  const calls = []
  const entry = {
    async put(body) { assert.equal(this, entry); calls.push(["put", body]) },
    async remove(body) { assert.equal(this, entry); calls.push(["remove", body]) },
  }
  const host = {experimental: {session: {instructions: {entry}}},
    get session() { throw new Error("不允许访问弃用别名") }}
  const intent = {host, view: makeView(), taskId: "T-1", sessionId: "ses-1", explicitUserAction: true}
  assert.equal((await applyInstructionEntry(intent)).ok, true)
  assert.equal((await removeInstructionEntry(intent)).ok, true)
  assert.deepEqual(calls.map(c => c[0]), ["put", "remove"])
})

test("SDK：仅有弃用别名而无正式路径时失败关闭，不擅自兼容发送", async () => {
  const calls = []
  const host = {session: {instructions: {entry: {put: async b => calls.push(b)}}}}
  const result = await applyInstructionEntry({host, view: makeView(), taskId: "T-1", sessionId: "ses-1", explicitUserAction: true})
  assert.equal(result.code, "HOST_UNAVAILABLE")
  assert.deepEqual(calls, [])
})

test("SDK：GUI提示使用description与default，不用TUI的message/info形状", async () => {
  const commands = [], toasts = []
  const cleanup = makeGuiPlugin({options: {}, add: (_token, create) => commands.push(create()),
    toast: {show: options => {
      assert.equal(Object.hasOwn(options, "message"), false)
      assert.equal(typeof options.description, "string")
      assert.ok(["default", "success", "error", "loading"].includes(options.variant))
      toasts.push(options)
    }}}, {Command: Symbol("Command")})
  await commands.find(c => c.id === "status").run()
  assert.equal(toasts.length, 1)
  assert.match(toasts[0].description, /零 I\/O/)
  cleanup()
})

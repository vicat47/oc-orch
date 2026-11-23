/**
 * 入口与包结构静态校验（不 import SDK，避免离线缺模块；真实加载未验）。
 * 校验点：
 * - tui.js 只 import @opencode/plugin/tui 与同目录核心，default-export Plugin.define；
 * - gui.js 只 import @opencode/plugin/gui 与同目录核心（源码候选，未构建/未导出）；
 * - 不存在 server 部分 / RPC / prompt 准入 hook / Enter 绑定；
 * - package.json 零依赖、type=module、只导出 ./tui（不含未构建的 ./gui-experimental）。
 */
import { test } from "node:test"
import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { fileURLToPath } from "node:url"
import path from "node:path"

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..")

function read(rel) {
  return readFileSync(path.join(root, rel), "utf8")
}

const SRC_FILES = ["src/bridge.js", "src/project-control.js", "src/operator-turn.js", "src/instructions.js", "src/tui-core.js", "src/tui.js", "src/gui-core.js", "src/gui.js"]

test("入口只从允许的 SDK specifier 导入；不引入 server/RPC/prompt 副作用", () => {
  const tui = read("src/tui.js")
  assert.match(tui, /import \{ Plugin \} from "@opencode\/plugin\/tui"/)
  assert.match(tui, /Plugin\.define\(\{/)
  assert.match(tui, /id: "oc-orch\.delivery-policy"/)
  assert.equal((tui.match(/from "@opencode\/plugin"/g) ?? []).length, 0, "不 import server SDK")

  const gui = read("src/gui.js")
  assert.match(gui, /import \{ Command, Plugin \} from "@opencode\/plugin\/gui"/)
  assert.match(gui, /Plugin\.define\(/)
})

test("全部业务源码不含 prompt 准入 hook、Enter 绑定、begin/finish turn、server RPC", () => {
  for (const f of SRC_FILES) {
    const src = read(f)
    assert.doesNotMatch(src, /session\.hook/, `${f}: 无 prompt hook`)
    assert.doesNotMatch(src, /bind:\s*["']enter["']/i, `${f}: 无 Enter 替换`)
    assert.doesNotMatch(src, /begin_user_turn|finish_user_turn|trusted\s*:\s*true/i, `${f}: 无 turn 副作用`)
    assert.doesNotMatch(src, /client\.rpc|\.rpc\(/, `${f}: 无 server RPC 写中继`)
  }
})

test("核心（bridge/instructions/tui-core/gui-core）不 import OpenCode SDK", () => {
  for (const f of ["src/bridge.js", "src/instructions.js", "src/tui-core.js", "src/gui-core.js"]) {
    const src = read(f)
    assert.doesNotMatch(src, /from "@opencode\/plugin/, `${f}: 核心必须零 SDK import`)
  }
})

test("不 guess sessionID：无 router.current / 未公开路由结构引用", () => {
  for (const f of SRC_FILES) {
    const src = read(f)
    assert.doesNotMatch(src, /router\.current|\.router\b/, `${f}: 不使用 router.current 猜 sessionID`)
  }
})

test("package.json：type=module、零 dependencies、只导出 ./tui", () => {
  const pkg = JSON.parse(read("package.json"))
  assert.equal(pkg.private, true)
  assert.equal(pkg.type, "module")
  assert.equal(Object.hasOwn(pkg, "dependencies"), false)
  assert.equal(Object.hasOwn(pkg, "devDependencies"), false)
  const exports = pkg.exports
  assert.ok(exports["./tui"], "导出 ./tui")
  assert.equal(Object.hasOwn(exports, "./gui-experimental"), false, "GUI bundle 未构建，不得伪称导出")
  assert.equal(Object.hasOwn(exports, "./rpc"), false, "无 server RPC 契约")
  assert.equal(Object.hasOwn(exports, "."), false, "根导出会被宿主发现为server part，纯helper不得占用")
  assert.equal(Object.hasOwn(exports, "./server"), false)
  assert.equal(exports["./bridge"], "./src/bridge.js")
  assert.equal(exports["./tui"], "./src/tui.js")
})

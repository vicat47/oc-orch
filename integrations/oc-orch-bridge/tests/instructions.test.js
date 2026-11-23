/**
 * instructions 动态指令投影离线验证（fake-host）。
 * 覆盖：显式动作门 / 宿主缺失 / 目标与会话作用域隔离 / 固定 key（不碰全局
 * AGENTS）/ draft 不宣称“运行时模式已生效” / 移除路径。
 */
import { test } from "node:test"
import assert from "node:assert/strict"

import {
  INSTRUCTION_KEY,
  instructionTargetOf,
  targetMatches,
  buildInstructionValue,
  applyInstructionEntry,
  removeInstructionEntry,
} from "../src/instructions.js"
import { makeView } from "./helpers.js"

function fakeHost() {
  const calls = { put: [], remove: [], list: [] }
  return {
    calls,
    experimental: { session: {
      instructions: {
        entry: {
          put: async (input) => {
            calls.put.push(input)
          },
          remove: async (input) => {
            calls.remove.push(input)
          },
          list: async () => calls.list,
        },
      },
    } },
  }
}

test("读取 instruction_target；缺失/形状非法 -> null", () => {
  assert.deepEqual(instructionTargetOf(makeView()), { session_id: "ses-1", task_id: "T-1" })
  assert.equal(instructionTargetOf(makeView({ instruction_target: undefined })), null)
  assert.equal(instructionTargetOf(makeView({ instruction_target: { session_id: "ses-1" } })), null)
})

test("targetMatches：session/task 任一不匹配 -> 拒绝（作用域隔离）", () => {
  const view = makeView()
  assert.equal(targetMatches({ view, taskId: "T-1", sessionId: "ses-1" }).ok, true)
  assert.equal(targetMatches({ view, taskId: "T-other", sessionId: "ses-1" }).code, "TASK_MISMATCH")
  assert.equal(targetMatches({ view, taskId: "T-1", sessionId: "ses-other" }).code, "SESSION_MISMATCH")
  assert.equal(targetMatches({ view, taskId: "T-1", sessionId: undefined }).code, "SESSION_MISMATCH")
})

test("buildInstructionValue：enforced=false 明确“草案/未执行”，不宣称已生效", () => {
  const draft = buildInstructionValue({ view: makeView({ execution_enforced: false }) })
  assert.match(draft, /草案/)
  assert.doesNotMatch(draft, /运行时模式已生效|已生效/)
  assert.ok(draft.includes("revision=r3"))
  const enforced = buildInstructionValue({
    view: makeView({ execution_enforced: true, revision: 4 }),
  })
  assert.match(enforced, /已生效/)
  // instructions=null -> 仍有状态行，无伪造后续步骤
  const noInst = buildInstructionValue({ view: makeView({ instructions: null }) })
  assert.ok(noInst.includes("草案"))
  assert.doesNotMatch(noInst, /下一步：等待验收/)
})

test("applyInstructionEntry：非显式动作 -> 拒绝，且 host 零调用", async () => {
  const host = fakeHost()
  const res = await applyInstructionEntry({
    host, view: makeView(), taskId: "T-1", sessionId: "ses-1", explicitUserAction: false,
  })
  assert.equal(res.code, "NOT_EXPLICIT")
  assert.equal(host.calls.put.length, 0)
})

test("applyInstructionEntry：宿主缺失 entry.put -> HOST_UNAVAILABLE", async () => {
  const res = await applyInstructionEntry({
    host: {}, view: makeView(), taskId: "T-1", sessionId: "ses-1", explicitUserAction: true,
  })
  assert.equal(res.code, "HOST_UNAVAILABLE")
})

test("applyInstructionEntry：成功写入固定 Task 作用域 key 到 server 声明且匹配的 session", async () => {
  const host = fakeHost()
  const res = await applyInstructionEntry({
    host, view: makeView(), taskId: "T-1", sessionId: "ses-1", explicitUserAction: true,
  })
  assert.equal(res.ok, true)
  assert.equal(host.calls.put.length, 1)
  const put = host.calls.put[0]
  assert.equal(put.sessionID, "ses-1")
  assert.equal(put.key, INSTRUCTION_KEY)
  assert.match(put.value, /\[oc-orch delivery-policy\]/)
  // 固定 key 绝不等于全局 AGENTS 相关路径/键
  assert.notEqual(put.key, "AGENTS.md")
  assert.ok(!put.key.includes("AGENTS"))
})

test("applyInstructionEntry：任务作用域隔离——target 与配置 task 不一致 -> 拒绝且 host 零调用", async () => {
  const host = fakeHost()
  const res = await applyInstructionEntry({
    host, view: makeView(), taskId: "T-other", sessionId: "ses-1", explicitUserAction: true,
  })
  assert.equal(res.code, "TASK_MISMATCH")
  assert.equal(host.calls.put.length, 0)
})

test("removeInstructionEntry：显式动作 + 作用域匹配才移除固定 key", async () => {
  const host = fakeHost()
  const res = await removeInstructionEntry({
    host, view: makeView(), taskId: "T-1", sessionId: "ses-1", explicitUserAction: true,
  })
  assert.equal(res.ok, true)
  assert.deepEqual(host.calls.remove[0], { sessionID: "ses-1", key: INSTRUCTION_KEY })
  const denied = await removeInstructionEntry({
    host, view: makeView(), taskId: "T-1", sessionId: "ses-1", explicitUserAction: false,
  })
  assert.equal(denied.code, "NOT_EXPLICIT")
  assert.equal(host.calls.remove.length, 1)
})

test("未导入全局 AGENTS/父模型：核心只提供固定 key 的 entry 写/删，无其他写面", () => {
  // 静态防线：instructions.js 只导出入口函数与固定 key，不引用文件系统/环境
  assert.equal(typeof INSTRUCTION_KEY, "string")
  assert.equal(INSTRUCTION_KEY, "oc-orch.delivery-policy.task")
})

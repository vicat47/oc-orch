/**
 * 动态会话指令投影（纯逻辑，零运行时依赖）
 *
 * 依据父级契约：只在「显式用户动作之后」且「server 返回 instruction_target
 * 且与配置/选定的 session、task 匹配」时，调用宿主 OpenCode client
 * experimental.session.instructions.entry.put，写**固定 Task 作用域 key** 一条指令条目。
 *
 * 边界（本模块 + tests 离线验证）：
 * - 绝不改全局 AGENTS.md / 共享父模型 / 任意 session/turn 覆盖链；只写固定 key。
 * - 不做费用/权限设置：条目只是动态指令投影（后续步骤展示），不授予资源。
 * - draft/未执行（execution_enforced=false）时明确标注“草案/未执行”，
 *   禁止把“运行时模式已生效”当作事实展示。
 * - 指令条目无跨窗口原子 CAS（未证）；晚到文本只作**非权威投影**，不可当作
 *   策略许可或执行证据；shared coordinator 绝不因会话指令而改绑。
 * - 不在宿主加载时调用；宿主 client 仅在命令处理中按需触碰。
 */
export const INSTRUCTION_KEY = "oc-orch.delivery-policy.task"

/**
 * @typedef {{host?: import("@opencode/client").OpenCodeClient, view: any,
 * taskId: string, sessionId: string, explicitUserAction?: boolean, entryKey?: string,
 * refreshView?: () => Promise<any>, canApply?: () => boolean}} InstructionIntent
 */

/** 读取 server 返回的指令目标；缺失/形状非法返回 null。 */
export function instructionTargetOf(view) {
  const t = view && typeof view === "object" ? view.instruction_target : null
  if (!t || typeof t !== "object") return null
  const session_id = typeof t.session_id === "string" ? t.session_id : null
  const task_id = typeof t.task_id === "string" ? t.task_id : null
  if (!session_id || !task_id) return null
  return { session_id, task_id }
}

/**
 * 目标与调用方范围隔离校验：
 * - 必须存在 instruction_target；
 * - sessionId（配置 sessionID 或用户显式选定）必须与 target.session_id 逐字一致；
 * - taskId（配置 taskScope 或当前任务）必须与 target.task_id 逐字一致。
 */
export function targetMatches({ view, taskId, sessionId }) {
  if (view?.scope !== "task" || view.scope_id !== taskId || view.managed !== true) {
    return { ok: false, code: "TASK_MISMATCH", message: "指令视图本身必须属于当前Task" }
  }
  const target = instructionTargetOf(view)
  if (!target) {
    return { ok: false, code: "NO_TARGET", message: "server 未返回 instruction_target" }
  }
  if (typeof sessionId !== "string" || target.session_id !== sessionId) {
    return {
      ok: false,
      code: "SESSION_MISMATCH",
      message: `目标 session ${JSON.stringify(target.session_id)} 与配置/选择 ${JSON.stringify(sessionId ?? null)} 不一致`,
    }
  }
  if (typeof taskId !== "string" || target.task_id !== taskId) {
    return {
      ok: false,
      code: "TASK_MISMATCH",
      message: `目标 task ${JSON.stringify(target.task_id)} 与当前 task ${JSON.stringify(taskId ?? null)} 不一致`,
    }
  }
  return { ok: true, target }
}

/**
 * 构造条目 value：固定 key 的 Task 作用域指令文本。
 * execution_enforced=false 时明确标记“草案/未执行”，不宣称“运行时模式已生效”。
 * 有 server 提供的 instructions 时优先采用为后续步骤正文。
 */
export function buildInstructionValue({ view }) {
  const enforced = view?.execution_enforced === true
  const policy = view?.version?.policy ?? {}
  const status = enforced
    ? "本地受管输入许可已生效（execution_enforced=true；非全链费用/内部调用保证）"
    : "草案/未执行：execution_enforced=false，尚未成为运行约束"
  const limits = policy.limits ?? {}
  const lines = [
    `[oc-orch delivery-policy] task ${view?.scope_id ?? "?"} revision=r${view?.revision ?? "?"}`,
    `preset=${policy.preset ?? "-"} stage=${policy.stage ?? "-"} limits={max_parallel:${limits.max_parallel ?? "-"},max_candidates:${limits.max_candidates ?? "-"},max_challenge_rounds:${limits.max_challenge_rounds ?? "-"}} ${status}`,
  ]
  if (typeof view?.instructions === "string" && view.instructions.trim() !== "") {
    lines.push(view.instructions)
  }
  return lines.join("\n")
}

/**
 * 显式用户动作后写入固定 key 的动态指令（官方 client 方法，宿主解析）。
 * host使用2.2.5 SDK的experimental.session.instructions.entry，不走已弃用别名。
 * 失败/未满足范围时返回 {ok:false, code, message}，绝不 fallback 到改全局文件。
 * @param {InstructionIntent} intent
 */
export async function applyInstructionEntry({
  host, view, taskId, sessionId, explicitUserAction,
  entryKey = INSTRUCTION_KEY, refreshView, canApply = () => true,
}) {
  if (explicitUserAction !== true) {
    return { ok: false, code: "NOT_EXPLICIT", message: "仅可在显式用户动作后同步指令" }
  }
  if (entryKey !== INSTRUCTION_KEY) return {ok: false, code: "FIXED_KEY_REQUIRED", message: "只允许固定Task指令key"}
  if (typeof refreshView === "function") {
    const current = await refreshView()
    if (!current.ok) return current
    view = current.view
  }
  if (!canApply()) return {ok: false, code: "DISPOSED", message: "指令同步已取消/被更新动作替代"}
  if (view?.read_only === true) return {ok: false, code: "SERVER_READ_ONLY", message: "服务端只读，不能写指令投影"}
  const entry = host?.experimental?.session?.instructions?.entry
  const putFn = entry?.put
  if (typeof putFn !== "function") {
    return {
      ok: false,
      code: "HOST_UNAVAILABLE",
      message: "宿主 client 不可用：缺少 experimental.session.instructions.entry.put",
    }
  }
  const matched = targetMatches({ view, taskId, sessionId })
  if (!matched.ok) return matched
  const value = buildInstructionValue({ view })
  await putFn.call(entry, { sessionID: matched.target.session_id, key: entryKey, value })
  return { ok: true, put: { sessionID: matched.target.session_id, key: entryKey, value } }
}

/**
 * 显式用户动作后移除固定 key（例如 server 返回 instructions=null 且用户确认清理）。
 * 作用域校验与 put 一致。
 * @param {InstructionIntent} intent
 */
export async function removeInstructionEntry({
  host, view, taskId, sessionId, explicitUserAction,
  entryKey = INSTRUCTION_KEY,
}) {
  if (explicitUserAction !== true) {
    return { ok: false, code: "NOT_EXPLICIT", message: "仅可在显式用户动作后清理指令" }
  }
  if (entryKey !== INSTRUCTION_KEY) return {ok: false, code: "FIXED_KEY_REQUIRED", message: "只允许固定Task指令key"}
  const entry = host?.experimental?.session?.instructions?.entry
  const removeFn = entry?.remove
  if (typeof removeFn !== "function") {
    return {
      ok: false,
      code: "HOST_UNAVAILABLE",
      message: "宿主 client 不可用：缺少 experimental.session.instructions.entry.remove",
    }
  }
  const matched = targetMatches({ view, taskId, sessionId })
  if (!matched.ok) return matched
  await removeFn.call(entry, { sessionID: matched.target.session_id, key: entryKey })
  return { ok: true, removed: { sessionID: matched.target.session_id, key: entryKey } }
}

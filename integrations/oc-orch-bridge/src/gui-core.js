/**
 * GUI 命令核心（纯 JS，无 JSX；可离线测试，ctx 由测试注入 fake）。
 *
 * 官方依据（V2 GUI，experimental）：`./gui-experimental` 入口 default-export
 * `Plugin.define`，SDK 由 OpenCode Desktop 宿主解析 `@opencode/plugin/gui`。
 * 窗口 setup 通过 `ctx.add(Command, …)` 注册命令，用 `ctx.dialogs.confirm/alert/
 * prompt/select` 与 `ctx.toast.show` 交互——均无需 JSX。
 *
 * 诚实边界：GUI入口必须以官方 `@opencode/plugin/gui/vite` 预设构建单CJS。
 * 隔离类型/构建与真实Desktop/浏览器加载分开记录，不能用构建或fake-host通过
 * 代替实际GUI加载；默认写关闭，范围与未验证项见README及SDK/宿主证据。
 *
 * 真实边界（父级核验）：oc-orch Web 仅允许同源 Origin+CSRF，未启 CORS/OPTIONS；
 * TUI 的 Node fetch 可直连，但 OpenCode GUI/浏览器自身 origin 直连本机 oc-orch
 * 必有跨源/预检限制。因此 GUI 写命令（switch/replay）默认**只读不可用**：
 * 仅当显式配置 allowGuiWrites=true（且走已核验同源通道）才允许；fake-fetch
 * 不代表真实跨源可用。
 *
 * 与 TUI 相同的安全口径：
 * - 加载零 I/O（不调用宿主 client、不发业务请求）；
 * - 未配置 baseUrl 零 I/O、默认禁写；不猜 sessionID（配置或显式输入）；
 * - 写前能力/身份匹配 + CAS；确认逐字一致不 trim；未知保原体可复核；
 * - 取消/卸载中止读并忽略晚到 UI；已发 POST 取消按结果未知处理；
 * - 不绑 Enter、无 prompt 准入副作用、不调用 server 业务 RPC。
 */
import {
  DeliveryBridge,
  normalizeConfig,
  makeOperationId,
  PRESETS,
  STAGES,
  assertWritePreconditions,
} from "./bridge.js"
import {
  INSTRUCTION_KEY,
  applyInstructionEntry,
  instructionTargetOf,
} from "./instructions.js"

/**
 * 构建 GUI 插件窗口核心（context = GUI window setup 的 ctx）。
 * ctx 最小形状：{ id, options, add, dialogs:{confirm,alert,prompt,select}, toast:{show}, client }。
 * Command 为 GUI SDK 的注册表令牌（由入口在 setup 内传入，保持核心不带 SDK import）。
 * 返回清理函数。
 * @param {import("@opencode/plugin/gui").Context} ctx
 * @param {{Command?: typeof import("@opencode/plugin/gui").Command, fetcher?: typeof fetch}} [options]
 */
export function makeGuiPlugin(ctx, { Command, fetcher } = {}) {
  const normalized = normalizeConfig(ctx?.options ?? {})
  const config = normalized.config
  const bridge = new DeliveryBridge(config, { fetcher })
  let disposed = false
  let epoch = 0
  let lastApplied = null

  /** @param {import("@opencode/plugin/gui").ToastOptions["variant"]} variant */
  const showToast = (variant, title, description) => {
    if (typeof ctx?.toast?.show === "function") {
      ctx.toast.show({ variant, title, description })
    }
  }
  const toastError = (title, description) => showToast("error", title, description)
  const toastInfo = (title, description) => showToast("default", title, description)
  const fresh = (myEpoch) => !disposed && myEpoch === epoch

  async function chooseScopeId(scope) {
    const configured = scope === "task" ? config.taskScope : config.projectScope
    if (configured) return configured
    if (typeof ctx?.dialogs?.prompt !== "function") return null
    const typed = await ctx.dialogs.prompt({
      title: scope === "task" ? "任务 ID（task_id）" : "项目 ID（project）",
      placeholder: scope === "task" ? "T-…" : "project-…",
    })
    if (typeof typed !== "string" || typed === "") return null
    return typed
  }

  async function chooseSelection(kind, view) {
    const curLimits = view?.version?.policy?.limits ?? {}
    const options = PRESETS.map((p) => ({ title: p, value: p }))
    if (kind === "task") options.unshift({ title: "inherit（继承项目默认快照）", value: "__inherit__" })
    if (typeof ctx?.dialogs?.select !== "function") return { cancelled: true }
    const picked = await ctx.dialogs.select({ title: "资源偏好 preset", options })
    if (!picked) return { cancelled: true }
    if (picked === "__inherit__") return { inherit: true }
    const stage = await ctx.dialogs.select({
      title: "工作阶段 stage",
      options: STAGES.map((s) => ({ title: s, value: s })),
    })
    if (!stage) return { cancelled: true }
    const defaultLimits = [
      curLimits.max_parallel ?? 4,
      curLimits.max_candidates ?? 4,
      curLimits.max_challenge_rounds ?? 2,
    ].join(",")
    const rawLimits =
      typeof ctx?.dialogs?.prompt === "function"
        ? await ctx.dialogs.prompt({
            title: "独立上限 limits（max_parallel,max_candidates,max_challenge_rounds）",
            placeholder: defaultLimits,
          })
        : null
    if (rawLimits === null || rawLimits === undefined) return { cancelled: true }
    const parts = rawLimits.split(",").map((s) => Number(s.trim()))
    if (parts.length !== 3 || parts.some((n) => Number.isNaN(n))) {
      return { error: "limits 需要三个整数，如 2,4,1" }
    }
    return { policy: { preset: picked, stage, limits: { max_parallel: parts[0], max_candidates: parts[1], max_challenge_rounds: parts[2] } } }
  }

  async function chooseSessionId() {
    if (config.sessionID) return config.sessionID
    if (typeof ctx?.dialogs?.prompt !== "function") return null
    const typed = await ctx.dialogs.prompt({ title: "目标 session ID（显式输入），留空跳过", placeholder: "ses_…" })
    if (typeof typed !== "string" || typed === "") return null
    return typed
  }

  async function statusCommand() {
    const myEpoch = ++epoch
    if (normalized.errors.length) return toastError("配置无效", normalized.errors.join("；"))
    if (!bridge.configured) return toastInfo("未配置", "未配置 baseUrl，插件零 I/O（不默认 8787）")
    const scope = config.taskScope ? "task" : "project"
    const id = config.taskScope ?? config.projectScope
    if (!id) return toastInfo("未配置", "未配置 projectScope/taskScope；请使用 switch 命令显式指定")
    const res = await bridge.readPolicy({ scope, id })
    if (!fresh(myEpoch)) return
    if (!res.ok) {
      // 跨源/网络/重定向/中止在 GUI/浏览器直连场景最可能发生；如实提示，不当作可用
      if (["NETWORK_UNKNOWN", "REDIRECT_REFUSED", "ABORTED"].includes(res.code)) {
        return toastError(
          `读取失败 [${res.code}]`,
          `${res.message}（GUI/浏览器直连本机 oc-orch 未走已核验同源通道，可能被跨源/预检限制；fake-fetch 不代表真实跨源可用）`,
        )
      }
      return toastError(`读取失败 [${res.code}]`, res.message)
    }
    const s = res.summary
    const enforced = s.execution_enforced
    const readOnly = s.read_only ? " 服务端 read_only（禁写）" : ""
    toastInfo(
      `oc-orch 策略 r${s.revision}`,
      `${s.scope_id}（${s.scope}，${s.managed ? "受管" : "未纳管"}）\npreset=${s.preset} stage=${s.stage}\nlimits={max_parallel:${s.limits.max_parallel ?? "-"},max_candidates:${s.limits.max_candidates ?? "-"},max_challenge_rounds:${s.limits.max_challenge_rounds ?? "-"}}\nrevision=r${s.revision} enforced=${enforced ? "是" : "否（草案，未生效）"}${readOnly}\nruntime: release=${s.release_id ?? "unmanifested"} writer=${s.writer_contract} guard=${s.writer_guard_valid ? "ok" : "bad"} policy_guard=${s.delivery_policy_guard_valid ? "ok" : "bad"}`,
    )
  }

  async function switchCommand(kind) {
    const myEpoch = ++epoch
    if (!bridge.configured) return toastInfo("未配置", "未配置 baseUrl，插件零 I/O（默认禁写）")
    if (config.writesEnabled !== true) return toastInfo("默认禁写", "需配置 writesEnabled=true 才可发送控制请求")
    // GUI/浏览器直连本机 oc-orch 有跨源/预检限制且未走已核验同源通道：写命令默认不可用
    if (config.allowGuiWrites !== true) {
      return toastInfo(
        "GUI 候选只读",
        "跨源通道未核验（oc-orch Web 仅同源 Origin+CSRF，未启 CORS/OPTIONS）；allowGuiWrites 未启用，写命令不可用（保持只读/失败关闭）",
      )
    }
    const id = await chooseScopeId(kind)
    if (id === null) return toastInfo("已取消", "未发送任何请求")
    if (!fresh(myEpoch)) return
    const read = await bridge.readPolicy({ scope: kind, id })
    if (!fresh(myEpoch)) return
    if (!read.ok) return toastError(`读取失败 [${read.code}]`, `${read.message}（未发送写请求）`)
    const preflight = bridge.planWrite({
      kind, id, view: read.view, operationId: "__preflight__", reason: "preflight",
      selection: kind === "task" ? { inherit: true } : {
        policy: { preset: "budget-first", stage: "standard", limits: { max_parallel: 1, max_candidates: 1, max_challenge_rounds: 0 } },
      }, preflight: true,
    })
    if (!preflight.ok) return toastError("拒绝", `[${preflight.code}] ${preflight.message}`)
    const sel = await chooseSelection(kind, read.view)
    if (sel.cancelled) return toastInfo("已取消", "已取消选择，未发送")
    if (sel.error) return toastError("输入无效", sel.error)
    if (!fresh(myEpoch)) return
    const operationId = makeOperationId()
    const reason = `gui:${kind}-switch`
    const planned = bridge.planWrite({
      kind, id, view: read.view, operationId, reason, selection: sel, preflight: true,
    })
    if (!planned.ok) return toastError("拒绝", `[${planned.code}] ${planned.message}`)
    const confirm =
      typeof ctx?.dialogs?.prompt === "function"
        ? await ctx.dialogs.prompt({ title: `确认${kind === "task" ? "任务" : "项目"}切换`, placeholder: `输入与 ${id} 完全一致的身份文本` })
        : null
    if (typeof confirm !== "string" || confirm === "") return toastInfo("已取消", "已取消确认，未发送")
    const replanned = bridge.planWrite({
      kind, id, view: read.view, operationId, reason, selection: sel, confirm,
    })
    if (!replanned.ok) return toastError("拒绝", `[${replanned.code}] ${replanned.message}`)
    if (!fresh(myEpoch)) return
    const res = await bridge.executeRecord(replanned.record)
    if (!fresh(myEpoch)) return
    if (!res.ok) return toastError(`写请求 [${res.code}]`, res.message)
    lastApplied = { view: res.current, operationId: res.operationId, kind }
    const enforced = res.execution_enforced === true
    toastInfo(
      "切换结果",
      `按回执 current 快照：preset=${res.currentSummary.preset} stage=${res.currentSummary.stage} revision=r${res.currentSummary.revision}${enforced ? "" : "（草案，未生效）"}`,
    )
    await offerInstructionSync(myEpoch)
  }

  async function offerInstructionSync(myEpoch) {
    if (!lastApplied) return
    const view = lastApplied.view
    const target = instructionTargetOf(view)
    if (!target) return
    const sessionId = await chooseSessionId()
    if (!fresh(myEpoch)) return
    if (!sessionId) return toastInfo("跳过同步", "未配置/选择目标 session，跳过指令同步")
    const taskId = lastApplied.kind === "task" ? view.scope_id : config.taskScope
    if (!taskId) return toastInfo("跳过同步", "指令同步仅支持任务策略（需 task 作用域）")
    const yes =
      typeof ctx?.dialogs?.confirm === "function"
        ? await ctx.dialogs.confirm({
            title: "同步动态指令？",
            confirm: "同步",
            description: `写入 session ${sessionId} 固定 key「${INSTRUCTION_KEY}」（Task 作用域；不改全局 AGENTS/共享父模型）`,
          })
        : false
    if (!fresh(myEpoch)) return
    if (!yes) return toastInfo("跳过同步", "本次不写指令条目")
    const res = await applyInstructionEntry({
      host: ctx.client,
      view,
      taskId,
      sessionId,
      explicitUserAction: true,
      canApply: () => fresh(myEpoch),
      refreshView: async () => {
        const latest = await bridge.readPolicy({scope: "task", id: taskId})
        if (!latest.ok) return latest
        const cap = assertWritePreconditions(latest.view, config.expected)
        return cap.ok ? latest : cap
      },
    })
    if (!fresh(myEpoch)) return
    if (!res.ok) return toastError("同步跳过", `[${res.code}] ${res.message}`)
    toastInfo("已写入", `指令条目 ${res.put.key}（下一执行步骤生效）`)
  }

  async function replayCommand() {
    const myEpoch = ++epoch
    if (config.allowGuiWrites !== true) {
      return toastInfo(
        "GUI 候选只读",
        "跨源通道未核验；allowGuiWrites 未启用，复核（原体重发）命令不可用（保持只读/失败关闭）",
      )
    }
    const ops = bridge.listUnknownOperations()
    if (ops.length === 0) return toastInfo("无未知", "没有结果未知的 operation")
    if (typeof ctx?.dialogs?.select !== "function") return
    const picked = await ctx.dialogs.select({
      title: "复核未知 operation（原 ID/body 显式重发）",
      options: ops.map((o) => ({
        title: `${o.operationId} ${o.kind}:${o.target?.scope_id}（${o.unknownReason}）`,
        value: o.operationId,
      })),
    })
    if (!picked) return
    const res = await bridge.replay(picked)
    if (!fresh(myEpoch)) return
    if (!res.ok) return toastError("复核未成功", `[${res.code}] ${res.message}`)
    toastInfo("复核成功", `revision=r${res.currentSummary.revision}${res.execution_enforced ? "" : "（草案，未生效）"}`)
  }

  async function syncCommand() {
    const myEpoch = ++epoch
    if (!lastApplied) return toastInfo("无切换", "尚无成功切换；请先执行 switch-task / switch-project")
    await offerInstructionSync(myEpoch)
  }

  const commands = [
    { id: "status", title: "oc-orch：查看交付策略", run: () => statusCommand() },
    { id: "switch-task", title: "oc-orch：切换任务交付策略", run: () => switchCommand("task") },
    { id: "switch-project", title: "oc-orch：切换项目默认交付策略", run: () => switchCommand("project") },
    { id: "replay", title: "oc-orch：复核未知结果 operation", run: () => replayCommand() },
    { id: "sync-instructions", title: "oc-orch：同步动态指令到当前任务 session", run: () => syncCommand() },
  ]

  if (typeof ctx?.add === "function" && Command != null) {
    for (const c of commands) {
      ctx.add(Command, () => ({
        id: c.id,
        title: c.title,
        keywords: "oc-orch delivery policy",
        suggested: true,
        run: c.run,
      }))
    }
  }

  return () => {
    disposed = true
    epoch += 1
    bridge.dispose()
  }
}

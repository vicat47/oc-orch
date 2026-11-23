/**
 * TUI 命令核心（可离线测试：不 import OpenCode SDK，context 由测试注入 fake）。
 *
 * 官方依据（V2）：TUI 插件 `./tui` 入口 default-export `Plugin.define`，SDK 由
 * 宿主从 `@opencode/plugin/tui` 运行时解析；本核心只依赖宿主注入的
 * context.{options,keymap.layer,ui.dialog,ui.toast,data.session,client} 形状。
 *
 * 设计口径（对齐父级契约与调研 §13）：
 * - 加载 setup 零 I/O：不调用宿主 client、不发业务请求，只注册命令；
 * - 未配置 baseUrl / scope 时命令零 I/O（不默认 8787），默认禁写；
 * - 不猜 sessionID（不使用任何未公开路由结构）：目标 session 只能来自配置
 *   sessionID 或用户在对话框中的显式输入/选择；
 * - 写前能力/身份匹配 + CAS；确认文本逐字相等（不 trim）；
 * - 未知结果保原 operation/body，允许显式复核；禁止自动重投；
 * - 取消/reload/unload 中止读并忽略晚到 UI；已发 POST 的取消按结果未知处理；
 * - 动态指令仅在显式命令动作后、target 匹配时写固定 Task 作用域 key；
 * - 不绑定 Enter、不加 prompt 准入副作用、不调用 server 业务 RPC。
 */
import {
  DeliveryBridge,
  normalizeConfig,
  makeOperationId,
  PRESETS,
  STAGES,
  summarizeView,
  assertWritePreconditions,
} from "./bridge.js"
import {
  INSTRUCTION_KEY,
  applyInstructionEntry,
  instructionTargetOf,
} from "./instructions.js"
import {ProjectControlBridge, createControlJournal} from "./project-control.js"
import {OperatorTurnBridge} from "./operator-turn.js"

const COMMAND_GROUP = "oc-orch"

function presetDescription(p) {
  if (p === "budget-first") return "预算优先（并行/候选/挑战轮次收敛）"
  if (p === "elastic") return "弹性加速（按资源可承受加速）"
  if (p === "latency-first") return "时效优先（关键路径缩短）"
  return p
}

/**
 * 构建 TUI 插件核心。返回清理函数（reload/disable/unload 时调用）。
 * context 最小形状：{ options, keymap:{layer}, ui:{dialog:{alert,confirm,prompt,select},toast:{show}}, data:{session:{list}}, client }。
 * @param {import("@opencode/plugin/tui").Plugin.Context} context
 * @param {{fetcher?: typeof fetch}} [options]
 */
export function makeTuiPlugin(context, { fetcher } = {}) {
  const normalized = normalizeConfig(context?.options ?? {})
  const config = normalized.config
  const bridge = new DeliveryBridge(config, { fetcher })
  const control = new ProjectControlBridge(config, {fetcher, journal: createControlJournal(context?.storage)})
  const userJournal = createControlJournal(context?.storage, {key: "operator-turn-intents-v1", actions: ["message"]})
  const userTurn = new OperatorTurnBridge(config, {fetcher, journal: userJournal})
  let controlBusy = false
  let disposed = false
  let epoch = 0
  // 最近一次成功应用（供“同步指令”命令使用），只保存视图，不保存回执外壳
  let lastApplied = null // { view, operationId }

  /** @type {Partial<import("@opencode/plugin/tui/context").Dialog>} */
  const dialogs = context?.ui?.dialog ?? {}
  /** @param {string} message @param {import("@opencode/plugin/tui/context").ToastVariant} [variant] */
  const showToast = (message, variant = "info") => {
    if (typeof context?.ui?.toast?.show === "function") {
      context.ui.toast.show({ title: "oc-orch", message, variant, duration: 4000 })
    }
  }
  const toastError = (message) => showToast(message, "error")
  const toastWarn = (message) => showToast(message, "warning")

  const fresh = (myEpoch) => !disposed && myEpoch === epoch

  /** 目标 scope id：配置优先，否则要求用户在对话框中显式输入（不猜测）。 */
  async function chooseScopeId(scope) {
    const configured = scope === "task" ? config.taskScope : config.projectScope
    if (configured) return configured
    if (typeof dialogs.prompt !== "function") return null
    const typed = await dialogs.prompt({
      title: scope === "task" ? "任务 ID（task_id）" : "项目 ID（project）",
      placeholder: scope === "task" ? "T-…" : "project-…",
    })
    if (typeof typed !== "string" || typed === "") return null
    return typed
  }

  /** 策略选择：preset -> stage -> limits（默认继承当前视图 limits）。 */
  async function chooseSelection(kind, view) {
    const curLimits = view?.version?.policy?.limits ?? {}
    /** @type {import("@opencode/plugin/tui/context").DialogSelectOption<string>[]} */
    const options = PRESETS.map((p) => ({ title: p, value: p, description: presetDescription(p) }))
    if (kind === "task") {
      options.unshift({ title: "inherit（继承项目默认快照）", value: "__inherit__" })
    }
    if (typeof dialogs.select !== "function") return { cancelled: true }
    const picked = await dialogs.select({ title: "资源偏好 preset", options })
    if (!picked) return { cancelled: true }
    if (picked === "__inherit__") return { inherit: true }
    const stage = await dialogs.select({
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
      typeof dialogs.prompt === "function"
        ? await dialogs.prompt({ title: "独立上限 limits（max_parallel,max_candidates,max_challenge_rounds）", placeholder: defaultLimits })
        : null
    if (rawLimits === null || rawLimits === undefined) return { cancelled: true }
    const parts = rawLimits.split(",").map((s) => Number(s.trim()))
    if (parts.length !== 3 || parts.some((n) => Number.isNaN(n))) {
      return { error: "limits 需要三个整数，如 2,4,1" }
    }
    return { policy: { preset: picked, stage, limits: { max_parallel: parts[0], max_candidates: parts[1], max_challenge_rounds: parts[2] } } }
  }

  /** 选择目标 session：配置 sessionID 优先；否则显式输入（不猜路由结构）。 */
  async function chooseSessionId() {
    if (config.sessionID) return config.sessionID
    let candidates = []
    try {
      if (typeof context?.data?.session?.list === "function") {
        const list = context.data.session.list()
        if (Array.isArray(list)) {
          candidates = list
            .map((s) => (typeof s === "string" ? { id: s, title: s } : s))
            .filter((s) => s && typeof s.id === "string")
        }
      }
    } catch {
      candidates = []
    }
    if (candidates.length > 0 && typeof dialogs.select === "function") {
      const picked = await dialogs.select({
        title: "选择目标 session（显式选择）",
        options: candidates.map((s) => ({ title: s.title || s.id, value: s.id })),
      })
      if (typeof picked === "string" && picked !== "") return picked
      return null
    }
    if (typeof dialogs.prompt !== "function") return null
    const typed = await dialogs.prompt({ title: "目标 session ID（显式输入），留空跳过", placeholder: "ses_…" })
    if (typeof typed !== "string" || typed === "") return null
    return typed
  }

  /** 预检用的最小合法选择（仅验证能力/身份/scope/CAS，不依赖当前策略存在）。 */
  function minimalSelection(kind) {
    if (kind === "task") return { inherit: true }
    return {
      policy: {
        preset: "budget-first",
        stage: "standard",
        limits: { max_parallel: 1, max_candidates: 1, max_challenge_rounds: 0 },
      },
    }
  }

  /** 状态命令：展示当前策略快照。 */
  async function statusCommand() {
    const myEpoch = ++epoch
    if (normalized.errors.length) return toastError(`配置无效：${normalized.errors.join("；")}`)
    if (!bridge.configured) return toastWarn("未配置 baseUrl，插件零 I/O（不默认 8787）")
    const scope = config.taskScope ? "task" : "project"
    const id = config.taskScope ?? config.projectScope
    if (!id) return toastWarn("未配置 projectScope/taskScope；请先配置或使用 switch 命令显式指定")
    const res = await bridge.readPolicy({ scope, id })
    if (!fresh(myEpoch)) return
    if (!res.ok) return toastError(`读取失败 [${res.code}] ${res.message}`)
    const summary = res.summary
    const lines = [
      `${summary.scope_id}（${summary.scope}，${summary.managed ? "受管" : "未纳管"}）`,
      `preset=${summary.preset} stage=${summary.stage}`,
      `limits={max_parallel:${summary.limits.max_parallel ?? "-"},max_candidates:${summary.limits.max_candidates ?? "-"},max_challenge_rounds:${summary.limits.max_challenge_rounds ?? "-"}}`,
      `revision=r${summary.revision} enforced=${summary.execution_enforced ? "是" : "否（草案，未生效）"}`,
      `runtime: release=${summary.release_id ?? "unmanifested"} writer=${summary.writer_contract} guard=${summary.writer_guard_valid ? "ok" : "bad"} policy_guard=${summary.delivery_policy_guard_valid ? "ok" : "bad"}`,
      summary.instructions ? `指令：${summary.instructions}` : null,
    ].filter((x) => x !== null)
    if (typeof dialogs.alert === "function") {
      await dialogs.alert({ title: `oc-orch 交付策略 r${summary.revision}`, message: lines.join("\n") })
    }
  }

  /**
   * 切换流程（显式用户动作）：读当前 -> 能力/身份预检 -> 选择 -> 确认
   * （逐字一致、不 trim）-> 发送 -> 按 current 真实快照展示；成功后询问是否\n * 同步动态指令（需 server 返回 instruction_target 且 session/task 匹配）。\n */
  async function switchCommand(kind) {
    const myEpoch = ++epoch
    if (!bridge.configured) return toastWarn("未配置 baseUrl，插件零 I/O（默认禁写）")
    if (config.writesEnabled !== true) {
      return toastWarn("默认禁写：需配置 writesEnabled=true 才可发送控制请求")
    }
    const id = await chooseScopeId(kind)
    if (id === null) return toastWarn("已取消，未发送任何请求")
    if (disposed || myEpoch !== epoch) return
    const read = await bridge.readPolicy({ scope: kind, id })
    if (!fresh(myEpoch)) return
    if (!read.ok) return toastError(`读取失败 [${read.code}] ${read.message}（未发送写请求）`)
    // 能力/身份预检（确认弹窗之前），通过后再进入选择
    const preflight = bridge.planWrite({
      kind, id, view: read.view, operationId: "__preflight__", reason: "preflight",
      selection: minimalSelection(kind), preflight: true,
    })
    if (!preflight.ok) return toastError(`拒绝 [${preflight.code}] ${preflight.message}`)
    const sel = await chooseSelection(kind, read.view)
    if (sel.cancelled) return toastWarn("已取消选择，未发送")
    if (sel.error) return toastError(sel.error)
    if (disposed || myEpoch !== epoch) return
    const operationId = makeOperationId()
    const reason = `tui:${kind}-switch`
    const planned = bridge.planWrite({
      kind, id, view: read.view, operationId, reason, selection: sel, preflight: true,
    })
    if (!planned.ok) return toastError(`拒绝 [${planned.code}] ${planned.message}`)
    // 精确确认：逐字相等、不 trim；取消则未发送
    const confirm =
      typeof dialogs.prompt === "function"
        ? await dialogs.prompt({ title: `确认${kind === "task" ? "任务" : "项目"}切换`, placeholder: `输入与 ${id} 完全一致的身份文本` })
        : null
    if (typeof confirm !== "string" || confirm === "") return toastWarn("已取消确认，未发送")
    const replanned = bridge.planWrite({
      kind, id, view: read.view, operationId, reason, selection: sel, confirm,
    })
    if (!replanned.ok) return toastError(`拒绝 [${replanned.code}] ${replanned.message}`)
    if (disposed || myEpoch !== epoch) return
    const res = await bridge.executeRecord(replanned.record)
    if (!fresh(myEpoch)) return
    if (!res.ok) {
      return toastError(`写请求 [${res.code}] ${res.message}`)
    }
    lastApplied = { view: res.current, operationId: res.operationId, kind }
    const enforced = res.execution_enforced === true
    showToast(
      `切换结果（按回执 current 快照）：preset=${res.currentSummary.preset} stage=${res.currentSummary.stage} revision=r${res.currentSummary.revision}${enforced ? "" : "（草案，未生效）"}`,
      enforced ? "success" : "info",
    )
    await offerInstructionSync(myEpoch)
  }

  /** 同步动态指令：仅在显式 command 动作后、target 匹配时写固定 Task 作用域 key。 */
  async function offerInstructionSync(myEpoch) {
    if (!lastApplied) return
    const view = lastApplied.view
    const target = instructionTargetOf(view)
    if (!target) return // server 未返回 instruction_target：跳过
    const sessionId = await chooseSessionId()
    if (disposed || myEpoch !== epoch) return
    if (!sessionId) return toastWarn("未配置/选择目标 session，跳过指令同步")
    const taskId = lastApplied.kind === "task" ? view.scope_id : config.taskScope
    if (!taskId) return toastWarn("指令同步仅支持任务策略（需 task 作用域）")
    const yes =
      typeof dialogs.confirm === "function"
        ? await dialogs.confirm({
            title: "同步动态指令？",
            message: `写入 session ${sessionId} 固定 key「${INSTRUCTION_KEY}」（Task 作用域；不改全局 AGENTS/共享父模型）`,
            label: { confirm: "同步", cancel: "跳过" },
          })
        : false
    if (disposed || myEpoch !== epoch) return
    if (!yes) return toastWarn("已跳过指令同步（本次不写）")
    const res = await applyInstructionEntry({
      host: context.client,
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
    if (!res.ok) return toastError(`指令同步跳过 [${res.code}] ${res.message}`)
    showToast(`已写入指令条目 ${res.put.key}（下一执行步骤生效）`, "success")
  }

  /** 显式复核未知 operation（同 ID 同体重发；不自动重投）。 */
  async function replayCommand() {
    const myEpoch = ++epoch
    const ops = bridge.listUnknownOperations()
    if (ops.length === 0) return toastWarn("没有结果未知的 operation")
    if (typeof dialogs.select !== "function") return
    const picked = await dialogs.select({
      title: "复核未知 operation（原 ID/body 显式重发）",
      options: ops.map((o) => ({
        title: `${o.operationId} ${o.kind}:${o.target?.scope_id}（${o.unknownReason}）`,
        value: o.operationId,
      })),
    })
    if (!picked) return
    const res = await bridge.replay(picked)
    if (!fresh(myEpoch)) return
    if (!res.ok) return toastError(`复核未成功 [${res.code}] ${res.message}`)
    const enforced = res.execution_enforced === true
    showToast(`复核成功：revision=r${res.currentSummary.revision}${enforced ? "" : "（草案，未生效）"}`, enforced ? "success" : "info")
  }

  /** 同步指令（独立命令；仅当存在最近成功切换时生效）。 */
  async function syncCommand() {
    const myEpoch = ++epoch
    if (!lastApplied) return toastWarn("尚无成功切换；请先执行 switch-task / switch-project")
    await offerInstructionSync(myEpoch)
  }

  /** 显式项目控制；不接管聊天、不终止worker、不清回合/provider门。 */
  async function controlCommand(action) {
    if (controlBusy) return toastWarn("已有控制操作在途，请先等回执或对账")
    if (normalized.errors.length) return toastError(`配置无效：${normalized.errors.join("；")}`)
    if (!control.configured || !config.writesEnabled) return toastWarn("未配置/默认禁写，零I/O")
    controlBusy = true
    const myEpoch = ++epoch
    try {
      const project = await chooseScopeId("project")
      if (!project || !fresh(myEpoch)) return
      const sessionId = await chooseSessionId()
      if (!sessionId || !fresh(myEpoch)) return
      const read = await control.readControl(project, sessionId)
      if (!fresh(myEpoch)) return
      if (!read.ok) return toastError(`[${read.code}] ${read.message}`)
      const preflight = control.preflight(read.view, project, sessionId)
      if (!preflight.ok) return toastError(`[${preflight.code}] ${preflight.message}`)
      const confirm = await dialogs.prompt?.({title: action === "pause" ? "暂停项目自动调谐（不中断worker）" : "继续项目自动调谐（不清其他门禁）",
        placeholder: `逐字输入项目 ${project}`})
      if (!fresh(myEpoch) || confirm == null || confirm === "") return
      const planned = control.plan({action, project, sessionId, view: read.view, confirm,
        operationId: makeOperationId(), reason: `tui:explicit-${action}`})
      if (!planned.ok) return toastError(`[${planned.code}] ${planned.message}`)
      const result = await control.executeControl(planned.record, {canSend: () => fresh(myEpoch)})
      if (!fresh(myEpoch)) return
      if (!result.ok) return toastError(`[${result.code}] ${result.message}`)
      showToast(`项目 ${project} 当前=${result.status} r${result.revision}；暂停保留worker，继续仍受回合/provider/任务守卫`, "success")
    } finally { controlBusy = false }
  }

  async function reconcileControlCommand() {
    if (controlBusy) return toastWarn("控制操作在途，暂不并发对账")
    if (!control.configured) return toastWarn("未配置baseUrl，零I/O")
    const myEpoch = ++epoch
    try {
      const pending = control.pending()
      if (!pending.length) return toastWarn("无此服务的未决项目控制操作")
      const picked = await dialogs.select?.({title: "原操作只读对账（不重发）",
        options: pending.map(e => ({title: `${e.action} ${e.body.project} ${e.operationId}`, value: e.operationId}))})
      if (!picked || !fresh(myEpoch)) return
      const result = await control.reconcile(picked)
      if (!fresh(myEpoch)) return
      if (!result.ok) return toastError(`[${result.code}] ${result.message}`)
      showToast(`原操作已确认r${result.applied_revision}；当前=${result.status} r${result.revision}（不是重放）`, "success")
    } catch { if (fresh(myEpoch)) toastError("意图台账不可读，不猜原操作、不重发") }
  }

  async function messageCommand(observe = false) {
    if (controlBusy || !userTurn.configured || !config.writesEnabled) return toastWarn("未配置/禁写/已有在途控制，未发送")
    controlBusy = true
    const mine = ++epoch
    try {
      if (observe) {
        const entries = userJournal.list().filter(e => e.baseUrl === config.baseUrl && e.status !== "refused")
        const selected = await dialogs.select?.({title: "观察原补充指令（不重发）", options: entries.map(e => ({title: `${e.body.project} ${e.operationId}`, value: e.operationId}))})
        if (!selected || !fresh(mine)) return
        const result = await userTurn.observeMessage(selected)
        if (fresh(mine)) showToast(result.ok ? (result.finished ? "原回合结束已核验；项目暂停/provider门保持原样" : `尚未结束：${result.reason}`) : `[${result.code}] ${result.message}`)
        return
      }
      const project = await chooseScopeId("project"), sessionId = await chooseSessionId()
      if (!project || !sessionId || !fresh(mine)) return
      const read = await userTurn.readControl(project, sessionId)
      if (!read.ok || !fresh(mine)) return toastWarn("控制状态未知，未发送")
      const text = await dialogs.prompt?.({title: "补充指令（本指定入口按用户处理；不解项目暂停）", placeholder: "输入本轮补充说明"})
      if (!text || !fresh(mine)) return
      const confirm = await dialogs.prompt?.({title: "确认补充指令目标项目", placeholder: project})
      if (!fresh(mine)) return
      const planned = userTurn.planMessage({project, sessionId, view: read.view, text, confirm, operationId: makeOperationId()})
      if (!planned.ok) return toastWarn(`[${planned.code}] ${planned.message}`)
      const result = await userTurn.sendMessage(planned.record, {canSend: () => fresh(mine)})
      if (fresh(mine)) showToast(result.ok ? `已受理原输入 ${result.operation.message_id}；结束前不注入自动待办，受理≠完成` : `[${result.code}] ${result.message}`)
    } catch { if (fresh(mine)) toastWarn("持久意图/观察失败，未自动重投") }
    finally { controlBusy = false }
  }

  /** @type {import("@opencode/plugin/tui/context").KeymapCommand[]} */
  const commands = [
    {
      id: "oc-orch.delivery-policy.status",
      title: "oc-orch：查看交付策略",
      group: COMMAND_GROUP,
      palette: true,
      run: () => statusCommand(),
    },
    {
      id: "oc-orch.delivery-policy.switch-task",
      title: "oc-orch：切换任务交付策略",
      group: COMMAND_GROUP,
      palette: true,
      run: () => switchCommand("task"),
    },
    {
      id: "oc-orch.delivery-policy.switch-project",
      title: "oc-orch：切换项目默认交付策略",
      group: COMMAND_GROUP,
      palette: true,
      run: () => switchCommand("project"),
    },
    {
      id: "oc-orch.delivery-policy.replay",
      title: "oc-orch：复核未知结果 operation",
      group: COMMAND_GROUP,
      palette: true,
      run: () => replayCommand(),
    },
    {
      id: "oc-orch.delivery-policy.sync-instructions",
      title: "oc-orch：同步动态指令到当前任务 session",
      group: COMMAND_GROUP,
      palette: true,
      run: () => syncCommand(),
    },
    {id: "oc-orch.project.pause", title: "oc-orch：暂停项目自动调谐", group: COMMAND_GROUP,
      palette: true, run: () => controlCommand("pause")},
    {id: "oc-orch.project.resume", title: "oc-orch：继续项目自动调谐", group: COMMAND_GROUP,
      palette: true, run: () => controlCommand("resume")},
    {id: "oc-orch.project.reconcile", title: "oc-orch：只读对账未决项目控制", group: COMMAND_GROUP,
      palette: true, run: () => reconcileControlCommand()},
    {id: "oc-orch.conversation.send", title: "oc-orch：显式补充指令", group: COMMAND_GROUP,
      palette: true, run: () => messageCommand()},
    {id: "oc-orch.conversation.observe", title: "oc-orch：观察补充指令完整结束", group: COMMAND_GROUP,
      palette: true, run: () => messageCommand(true)},
  ]

  // 注册命令：keymap layer 在禁用/reload/setup 失败时自动移除；不绑定任何按键
  if (typeof context?.keymap?.layer === "function") {
    context.keymap.layer(() => ({ mode: "global", commands }))
  }

  return () => {
    disposed = true
    epoch += 1 // 晚到 UI 一律忽略
    bridge.dispose()
    control.dispose()
    userTurn.dispose()
  }
}

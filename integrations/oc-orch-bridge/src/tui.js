/**
 * TUI 插件入口（V2）：host 从 `@opencode/plugin/tui` 解析 Plugin；2.2.5类型与
 * 隔离真实TUI已单独核验，实际范围见SDK/宿主证据；不注册个人配置。
 * 业务逻辑全部在 tui-core.js（离线可用 fake-host 验证），本文件保持薄包装。
 */
import { Plugin } from "@opencode/plugin/tui"
import { makeTuiPlugin } from "./tui-core.js"

export default Plugin.define({
  id: "oc-orch.delivery-policy",
  setup: (context) => makeTuiPlugin(context),
})

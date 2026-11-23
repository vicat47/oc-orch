/**
 * GUI 插件入口候选（V2，experimental）；隔离构建与真实GUI加载分别验收。
 *
 * 官方要求：`./gui-experimental` 必须是**预编译单个 CommonJS 文件**，用官方
 * Vite 预设 `@opencode/plugin/gui/vite`（gui() 插件）构建。默认package.json
 * 仍不导出 ./gui-experimental；隔离构建通过不代表GUI已加载或跨源通道可用。
 * 请勿在真实Desktop上直接引用本源码文件或自动启用GUI写入。
 *
 * 构建说明见 README「GUI（桌面/浏览器）入口候选」一节。
 *
 * 窗口 setup 同步注册命令；`main` 进程无需业务代码（无 IPC/原生菜单使用），
 * 故不定义 main。SDK 类型与真实 Desktop 加载未验证。
 */
import { Command, Plugin } from "@opencode/plugin/gui"
import { makeGuiPlugin } from "./gui-core.js"

export default Plugin.define({
  setup: (ctx) => makeGuiPlugin(ctx, { Command }),
})

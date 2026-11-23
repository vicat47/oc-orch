// 仅供显式隔离验收；不在插件加载时构建，不默认写仓库产物。
import path from "node:path"
import {fileURLToPath} from "node:url"
import {defineConfig} from "vite"
import {gui} from "@opencode/plugin/gui/vite"

const outFile = process.env.OC_ORCH_BRIDGE_GUI_OUT
if (!outFile || !path.resolve(outFile).startsWith("/tmp/opencode/")) {
  throw new Error("OC_ORCH_BRIDGE_GUI_OUT必须显式指定/tmp/opencode/内的隔离产物路径")
}

export default defineConfig({
  plugins: [gui({entry: fileURLToPath(new URL("../src/gui.js", import.meta.url)), outFile})],
})

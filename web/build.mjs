#!/usr/bin/env node
/**
 * oc-orch 前端资源构建：开发期 esbuild，生成物随 wheel/sdist 本地分发。
 *
 * 输入（固定，不允许占位 override，防止生产误打烟测入口）：
 *   - web/src/app.js      Lit 应用入口（含静态导入的各视图/工具模块）
 *   - web/src/styles.css  共享样式
 *   - web/index.html      同源页面外壳
 *
 * 输出（相对仓库根，字节可复现、无时间戳）：
 *   src/oc_orch/entrypoints/web_assets/
 *     app.js                  esbuild ESM 打包（不压缩，保留标识与可测试性）
 *     app.css                 bundle 提取的 CSS；无 CSS 导入时取 styles.css
 *     index.html              页面外壳（随 wheel 分发，服务端 HTML_PAGE 同名提供）
 *     THIRD_PARTY_LICENSES.txt 运行期依赖（lit 及其传递依赖）许可证
 *     manifest.json           仅列上述 4 件内容文件（sha256/size）+ 版本；
 *                             manifest 自身不入哈希清单（避免自包含导致不可复现）
 *
 * 用法：
 *   node build.mjs            构建
 *   node build.mjs --check    在临时目录重建，与仓库现状逐字节比对（含 manifest），
 *                             有差异即失败且不写产物
 *
 * 约束：产物字节完全由输入确定；`--check` 永不偷偷改已提交产物；依赖闭包在
 * 锁中缺失即失败（不得静默漏许可证）。
 */
import * as esbuild from "esbuild";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(here, "..");
const webDir = path.join(ROOT, "web");
const srcDir = path.join(webDir, "src");
const OUT_DIR = path.join(ROOT, "src", "oc_orch", "entrypoints", "web_assets");

const ENTRY = "./src/app.js";
const STYLES = path.join(srcDir, "styles.css");
const SHELL = path.join(webDir, "index.html");

// manifest 只收录内容文件；manifest.json 自身由 check 单独比对。
const CONTENT_FILES = ["app.js", "app.css", "index.html", "THIRD_PARTY_LICENSES.txt"];
const ALL_OUTPUTS = [...CONTENT_FILES, "manifest.json"];
const LICENSE_CANDIDATES = [
  "LICENSE", "LICENSE.md", "LICENSE.txt", "LICENSE.mit", "LICENSE.MIT",
  "LICENSE-MIT", "COPYING", "LICENSE-APACHE",
];

// esbuild 对位于 absWorkingDir 之外的模块（例如 node_modules 是符号链接、指向
// 真实目录在仓库外）会在文件头注释里打印绝对/跨目录路径，使产物字节依赖机器布局，
// 破坏 build:check 可复现性。这里把这类头注释统一归一化为 `// node_modules/<...>`：
//   - 物理位置在 webDir 内的依赖：esbuild 已输出 `// node_modules/<pkg>/...`（无斜杠前缀），
//     正则不匹配、原样保留；
//   - 符号链接/仓库外依赖：输出 `// <绝对或 ../ 路径>/node_modules/<pkg>/...`，
//     归一化为与前者完全相同的字节。
const NODE_MODULES_COMMENT = /^\/\/ .*\/node_modules\//gm;
function normalizeBundlePathComments(file) {
  const text = fs.readFileSync(file, "utf8");
  const next = text.replace(NODE_MODULES_COMMENT, "// node_modules/");
  if (next !== text) fs.writeFileSync(file, next, "utf8");
}

function sha256(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function runtimeDepClosure() {
  // 从已安装状态（node_modules/.package-lock.json）解析 lit 运行期依赖闭包；
  // 该文件由 `npm install` 生成并反映真实安装版；依赖节点在锁中缺失即失败，
  // 拒绝静默跳过而漏掉对应许可证。
  const installedLock = path.join(webDir, "node_modules", ".package-lock.json");
  const pick = fs.existsSync(installedLock) ? installedLock
                                            : path.join(webDir, "package-lock.json");
  const packages = readJson(pick).packages || {};
  const order = [];
  const seen = new Set();
  const queue = ["node_modules/lit"];
  while (queue.length) {
    const key = queue.shift();
    if (seen.has(key)) continue;
    seen.add(key);
    const meta = packages[key];
    const name = key.replace(/^node_modules\//, "") || "(root)";
    if (!meta) {
      throw new Error(
        `运行期依赖 ${name} 未出现在安装锁 ${path.relative(ROOT, pick)} 中；` +
        `请先在 web/ 下同步 npm 依赖（npm ci --ignore-scripts）`);
    }
    order.push({key, name, license: meta.license, version: meta.version});
    for (const dep of Object.keys(meta.dependencies || {})) {
      queue.push(`node_modules/${dep}`);
    }
  }
  if (!order.length) {
    throw new Error("已安装锁缺少 node_modules/lit，请先在 web/ 下同步 npm 依赖");
  }
  return order;
}

function licenseText(name, version, licenseField, pkgDir) {
  for (const cand of LICENSE_CANDIDATES) {
    const p = path.join(pkgDir, cand);
    if (fs.existsSync(p) && fs.statSync(p).isFile()) {
      const text = fs.readFileSync(p, "utf8").trim();
      return `${name} ${version} (${licenseField})\n\n${text}\n`;
    }
  }
  return `---\n${name} ${version}（license: ${licenseField}）\n本包未随 npm 分发 LICENSE 文件；以 node_modules 内 package.json license 字段为准。\n---\n`;
}

function buildLicenses(meta) {
  const header = [
    "oc-orch 前端运行期第三方依赖许可证",
    "",
    "本文件由 web/build.mjs 自动生成；其内容由 npm 锁定的 node_modules 确定。",
    "仅列出随 app.js 打包进入运行期的依赖（lit 及其传递依赖）。",
    "开发期工具（esbuild/jsdom/@playwright 等）不随 wheel/sdist 分发，不在运行期使用。",
    "",
    "生成清单（按依赖闭包顺序）：",
    ...meta.map((m, i) => `  ${i + 1}. ${m.name} ${m.version || "(version?)"}`),
    "",
    "==== 许可证文本 ====",
    "",
  ].join("\n");
  const parts = [];
  for (const m of meta) {
    const pkgDir = path.join(webDir, "node_modules", m.name);
    parts.push(licenseText(m.name, m.version || "?", m.license || "unknown", pkgDir));
  }
  return header + "\n" + parts.join("\n\n") + "\n";
}

async function buildOne(outDir) {
  fs.mkdirSync(outDir, {recursive: true});
  // 先移除历史 app.css：若 app.js 导入样式则由 esbuild 重新产出；否则显式回退复制
  // （避免“旧 app.css 已存在 → 跳过复制”导致 styles.css 变更后产物残留旧样式）。
  fs.rmSync(path.join(outDir, "app.css"), {force: true});
  await esbuild.build({
    entryPoints: [ENTRY],
    absWorkingDir: webDir,
    outfile: path.join(outDir, "app.js"),
    bundle: true,
    format: "esm",
    platform: "browser",
    target: ["es2022"],
    minify: false, // 不压缩：产物可读、标识可测、字节可复现
    sourcemap: false,
    logLevel: "error",
  });
  normalizeBundlePathComments(path.join(outDir, "app.js"));
  // app.js 未 import 任何 CSS 时 esbuild 不产出 app.css；显式回退到共享样式。
  if (!fs.existsSync(path.join(outDir, "app.css"))) {
    fs.copyFileSync(STYLES, path.join(outDir, "app.css"));
  }
  fs.copyFileSync(SHELL, path.join(outDir, "index.html"));

  const meta = runtimeDepClosure();
  fs.writeFileSync(path.join(outDir, "THIRD_PARTY_LICENSES.txt"),
                   buildLicenses(meta), "utf8");

  // manifest 仅列内容文件；manifest.json 自身不计入清单（避免自包含摘要）。
  const version = readJson(path.join(webDir, "package.json")).version;
  const files = {};
  for (const name of CONTENT_FILES) {
    const buf = fs.readFileSync(path.join(outDir, name));
    files[name] = {sha256: sha256(buf), size: buf.length};
  }
  const manifest = {schema: "oc-orch.web-assets.v1", version, files};
  fs.writeFileSync(path.join(outDir, "manifest.json"),
                   JSON.stringify(manifest, null, 2) + "\n", "utf8");
}

function summarize() {
  const manifest = readJson(path.join(OUT_DIR, "manifest.json"));
  const lines = CONTENT_FILES
    .map((n) => {
      const f = manifest.files[n];
      return `  ${n.padEnd(28)} ${f.sha256.slice(0, 16)}  ${f.size} B`;
    })
    .join("\n");
  const manifestBuf = fs.readFileSync(path.join(OUT_DIR, "manifest.json"));
  return `oc-orch web assets v${manifest.version}\n${lines}\n` +
         `  ${"manifest.json".padEnd(28)} ${sha256(manifestBuf).slice(0, 16)}  ${manifestBuf.length} B`;
}

async function check() {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "oc-orch-web-build-"));
  try {
    await buildOne(tmp);
    const diffs = [];
    for (const name of ALL_OUTPUTS) {
      const a = fs.readFileSync(path.join(OUT_DIR, name));
      const b = fs.readFileSync(path.join(tmp, name));
      if (!a.equals(b)) {
        diffs.push(`${name}: 仓库 ${sha256(a).slice(0, 16)} != 重建 ${sha256(b).slice(0, 16)}`);
      }
    }
    if (diffs.length) {
      console.error("build:check 失败——产物与源码/依赖不一致（请运行 npm run build 重新生成）：");
      for (const d of diffs) console.error("  - " + d);
      return 1;
    }
    console.log("build:check 通过：产物与当前源码一致。");
    console.log(summarize());
    return 0;
  } finally {
    fs.rmSync(tmp, {recursive: true, force: true});
  }
}

if (!fs.existsSync(path.join(webDir, ENTRY))) {
  console.error(`web/build.mjs: 入口不存在：${path.join(webDir, ENTRY)}`);
  process.exit(1);
}
if (!fs.existsSync(SHELL)) {
  console.error(`web/build.mjs: 页面外壳不存在：${SHELL}`);
  process.exit(1);
}

const checkMode = process.argv.includes("--check");
if (checkMode) {
  process.exit(await check());
}
await buildOne(OUT_DIR);
console.log(summarize());
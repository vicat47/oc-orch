// 有界收口定向回归：仅复测 run3 的 3 个真实失败项（触控尺寸 / 尝试 h2 溢出 / boot 残留），
// 并保留原 5s 刷新 key/data/details 保留性抽查。默认直接服务 builder 产出的最终 package 字节
// （web_assets 下 app.js/app.css/index.html），不做 esbuild 重打；跑时依赖最终 CSS 与本页。
// 用法：cd web && node browser/targeted.mjs [assetsDir]
//       默认 assetsDir = <repo>/src/oc_orch/entrypoints/web_assets（可用环境变量 OC_ORCH_WEB_ASSETS 覆盖）
// 产物：/tmp/opencode/lit-web-browser-targeted/{logs,screenshots,report.md}（run3 证据不覆盖）
// 退出码：0 全过 / 1 失败 / 2 资产缺失（依赖未就绪，有界即停）。
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import {fileURLToPath} from "node:url";
import {chromium} from "playwright";
import {startFake, LONG_TASK_ID, LONG_ATTEMPT_ID} from "./fake-api.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, "..", "..");
const DEFAULT_ASSETS = path.join(REPO_ROOT, "src", "oc_orch", "entrypoints", "web_assets");
const ASSETS_DIR = process.env.OC_ORCH_WEB_ASSETS || process.argv[2] || DEFAULT_ASSETS;

const RUN_DIR = process.env.OC_ORCH_WEB_BROWSER_RUN_DIR || "/tmp/opencode/lit-web-browser-targeted";
const SHOT_DIR = path.join(RUN_DIR, "screenshots");
const LOG_DIR = path.join(RUN_DIR, "logs");
for (const d of [SHOT_DIR, LOG_DIR]) fs.mkdirSync(d, {recursive: true});

// ---- 有界预检：最终包必须就绪，否则退出 2 并明确提示 ----
const NEED = ["index.html", "app.js", "app.css"];
const missing = NEED.filter(f => !fs.existsSync(path.join(ASSETS_DIR, f)));
if (missing.length) {
  console.log(`[targeted] 资产缺失（依赖 builder 最终包，本轮未运行）: ${missing.join(", ")}`);
  console.log(`[targeted] assetsDir 期望 = ${ASSETS_DIR}`);
  console.log(`[targeted] 可用 OC_ORCH_WEB_ASSETS 指向新版产物后重跑。退出码 2（有界即停，不扩测试）。`);
  process.exit(2);
}
for (const f of NEED) {
  const buf = fs.readFileSync(path.join(ASSETS_DIR, f));
  console.log(`[targeted] 服务最终包字节 ${f} bytes=${buf.length} sha256=${crypto.createHash("sha256").update(buf).digest("hex").slice(0, 16)}`);
}

const VIEWPORTS = [
  {name: "vp-360", width: 360, height: 740},
  {name: "vp-390", width: 390, height: 844},
  {name: "vp-768", width: 768, height: 1024},
  {name: "vp-1280", width: 1280, height: 800},
];
const LOG_LINES = [];
const log = line => { LOG_LINES.push(String(line)); process.stdout.write(line + "\n"); };
const findings = [];
const screenshotWarnings = [];

function recordScreenshot(name, err) {
  const msg = `${name} 截图失败: ${(err && err.message) || err}`;
  screenshotWarnings.push(msg);
  log(`  [warn] ${msg}`);
}

// ---------- 假 API server（复用 fake-api，服务 web_assets 静态字节） ----------
const writeLog = [];
const {port, close} = await startFake({bundleDir: "", writeLog, assetsDir: ASSETS_DIR});
const BASE = `http://127.0.0.1:${port}`;
log(`[targeted] fake API + 静态 web_assets 于 ${BASE}`);

const browser = await chromium.launch();
const browserVersion = browser.version();
console.log(`[targeted] chromium ${browserVersion}`);

let totalFail = 0;

async function qaViewport(vp) {
  const context = await browser.newContext({viewport: {width: vp.width, height: vp.height}});
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", e => errors.push(String(e)));
  page.on("console", m => { if (m.type() === "error") errors.push(m.text()); });
  page.on("dialog", d => d.accept());
  await page.addInitScript(() => {
    window.__flash = {load: false, fail: false};
    try {
      const obs = new MutationObserver(ms => {
        for (const m of ms) {
          if (m.type !== "childList") continue;
          for (const n of m.addedNodes) {
            if (n.nodeType !== 1) continue;
            const t = n.textContent || "";
            if (t.includes("正在从服务端读取数据")) window.__flash.load = true;
            if (t.includes("加载失败") || t.includes("无法读取详情")) window.__flash.fail = true;
          }
        }
      });
      obs.observe(document.documentElement, {childList: true, subtree: true});
    } catch (_) { /* ignore */ }
  });

  const fails = [];
  const ok = (name, cond, extra = "") => {
    const status = cond ? "PASS" : "FAIL";
    log(`  [${status}] ${vp.name} ${name}${extra ? " | " + extra : ""}`);
    if (!cond) fails.push(name + (extra ? " | " + extra : ""));
  };
  const rect = sels => page.evaluate(sels => {
    const out = [];
    for (const sel of sels) {
      for (const el of document.querySelectorAll(sel)) {
        const r = el.getBoundingClientRect();
        out.push({sel, text: (el.textContent || "").trim().replace(/\s+/g, " ").slice(0, 20), h: Math.round(r.height * 10) / 10, w: Math.round(r.width * 10) / 10, clsw: el.className || ""});
      }
    }
    return out;
  }, sels);
  const scrollW = () => page.evaluate(() => ({sw: document.documentElement.scrollWidth, cw: document.documentElement.clientWidth}));
  const hashNav = async h => { await page.evaluate(v => { location.hash = v; }, h); await page.waitForTimeout(150); };
  const waitSel = (sel, timeout = 8000) => page.waitForSelector(sel, {timeout});
  const checkTiny = (label, items) => {
    const bad = items.filter(i => i.h < 43.5 || i.w < 43.5);
    for (const b of bad) findings.push({vp: vp.name, label, sel: b.sel, text: b.text, h: b.h, w: b.w});
    return bad;
  };
  const shot = async name => {
    const f = path.join(SHOT_DIR, `${vp.name}-${name}.png`);
    try { await page.screenshot({path: f}); }
    catch (err) { recordScreenshot(`${vp.name} ${name}`, err); }
    return f;
  };
  // 44×44 触控口径：仅 ≤768（手机/平板）强制；1280 桌面原 30px 按钮只记录尺寸不判 44。
  const enforce44 = vp.width <= 768;
  const touchVerdict = (label, bad) => {
    if (enforce44) ok(`${label} 触控 ≥44×44（≤768）`, bad.length === 0, `${bad.length} 处不足`);
    else log(`  [info] ${vp.name} ${label} 触控 <44 记录=${bad.length} 处（桌面 1280 原 30px 按钮，仅记录不判 44）`);
  };

  try {
    // ---- 总览：全局溢出 / boot 残留 / 查看工作台+nav+copy 44×44 ----
    await page.goto(BASE + "/", {waitUntil: "domcontentloaded"});
    await waitSel("#overviewRoot .grid-cards .card");
    // boot 残留：最终 index 应为空 <oc-orch-app> + CSS :not(:defined)::before 占位，定义后无 p.boot-note/加载文案。
    const boot = await page.evaluate(() => ({
      defined: !!customElements.get && !!customElements.get("oc-orch-app"),
      bootP: !!document.querySelector("p.boot-note"),
      bootText: (document.querySelector("oc-orch-app")?.textContent || "").includes("正在加载 oc-orch 控制面"),
    }));
    ok("app 已定义且无 boot-note 残留（空壳+pseudo 占位）", boot.defined && !boot.bootP && !boot.bootText, JSON.stringify(boot));
    const ov = await scrollW();
    ok("总览无全局横滚溢出", ov.sw <= ov.cw + 1, `sw=${ov.sw} cw=${ov.cw}`);
    const gov = await rect(["a.button", "nav a", "button.copy"]);
    const govBad = checkTiny("总览 a.button/nav/copy", gov);
    touchVerdict("总览 a.button/nav/copy", govBad);
    await shot("overview");

    // ---- 诊断：表格内 label/unlock/managed 与 ACK/sup/send 44×44；局部横滚不全局溢出 ----
    await hashNav("#/diagnostics");
    await waitSel("#diag-notif");
    const dsel = ["#alertTable label", "#gatesRoot [data-act='unlock']", "[data-managed]", "#ackCheckedBtn", "#ackAlertsBtn", "[data-act='supstart']", "[data-act='supstop']", "#sendForm button[type='submit']"];
    const dmu = await rect(dsel);
    const dBad = checkTiny("诊断 表格内/写控件", dmu);
    touchVerdict("诊断 表格内 label/unlock/managed 与 ACK/sup/send", dBad);
    const dio = await scrollW();
    ok("诊断无全局横滚溢出（表格仅局部横滚）", dio.sw <= dio.cw + 1, `sw=${dio.sw} cw=${dio.cw}`);
    // 5s 刷新保留：details / 勾选 / 根节点 / 数据更新 / 不闪 loading（分阶段观测）
    await page.locator("#alertTable details summary").first().click();
    await page.locator("#notifTable input.noteck").first().uncheck();
    const rootRef = await page.evaluate(() => { window.__dr = document.querySelector("#diag-notif"); return true; });
    const title = await page.textContent("#alertTable tbody tr >> nth=0");
    await page.evaluate(() => { window.__flash.load = false; window.__flash.fail = false; });
    await page.waitForTimeout(5600);
    ok("刷新后 details 仍展开", await page.locator("#alertTable details").first().evaluate(d => d.open));
    ok("刷新后勾选保留", await page.locator("#notifTable input.noteck").first().isChecked().then(v => !v));
    ok("刷新后根节点同一", await page.evaluate(() => document.querySelector("#diag-notif") === window.__dr));
    ok("刷新后数据原地更新", (await page.textContent("#alertTable tbody tr >> nth=0")) !== title);
    ok("后台刷新不闪 loading（分阶段观测）", !(await page.evaluate(() => window.__flash.load)));
    await shot("diagnostics-open");

    // ---- 尝试详情：h2 长 id 不可断溢出已修（documentElement 真实 scrollWidth）----
    await hashNav(`#/attempt/${encodeURIComponent(LONG_ATTEMPT_ID)}`);
    await waitSel("#attemptRoot h2");
    const ao = await scrollW();
    ok("尝试详情无全局横滚溢出（h2 长 id 换行兜底）", ao.sw <= ao.cw + 1, `sw=${ao.sw} cw=${ao.cw}`);
    const h2 = await page.evaluate(() => { const h = document.querySelector("#attemptRoot h2"); const r = h.getBoundingClientRect(); return {rw: Math.round(r.width), right: Math.round(r.right), vw: window.innerWidth, text: h.textContent.slice(0, 18)}; });
    ok("尝试 h2 宽度收敛在视口内", h2.right <= h2.vw + 1, JSON.stringify(h2));
    await shot("attempt");

    ok("targeted 页无 pageerror/console error", errors.length === 0, JSON.stringify(errors.slice(0, 3)));
  } catch (err) {
    fails.push(`EXCEPTION: ${err.message}`);
    try { await page.screenshot({path: path.join(SHOT_DIR, `${vp.name}-fail.png`)}); }
    catch (e) { recordScreenshot(`${vp.name} fail`, e); }
    log(`  [FAIL] ${vp.name} 异常: ${err.message}`);
  } finally {
    await context.close();
  }
  totalFail += fails.length;
  return {name: vp.name, fails};
}

console.log("\n======== targeted 有界回归 ========");
const results = [];
for (const vp of VIEWPORTS) {
  log(`\n-- ${vp.name} (${vp.width}x${vp.height}) --`);
  results.push(await qaViewport(vp));
}

await browser.close();
await close();

// ---------- 报告 ----------
const summary = results.map(r => `${r.name}: ${r.fails.length ? "FAIL(" + r.fails.length + ")" : "PASS"}`).join("  ");
log(`\n======== 汇总 ========\n${summary}\n总失败断言: ${totalFail}\n触控不足项: ${findings.length}`);
fs.writeFileSync(path.join(LOG_DIR, "targeted.log"), LOG_LINES.join("\n") + "\n");
const report = [
  "# lit-web targeted 定向回归（run3 三真实失败项收口验证）",
  "",
  "- 时间: " + new Date().toISOString(),
  "- Chromium: " + browserVersion,
  "- 命令: cd web && node browser/targeted.mjs [assetsDir]",
  "- 服务资产: " + ASSETS_DIR + "（builder 最终 package 字节，非 esbuild 临时）",
  "- 视口: " + VIEWPORTS.map(v => `${v.name}(${v.width}x${v.height})`).join(" "),
  "- 退出码: " + (totalFail ? 1 : 0),
  "",
  "## 逐视口",
  "",
  results.map(r => `- ${r.name}: ${r.fails.length ? "**FAIL** — " + r.fails.join("; ") : "**PASS**"}`).join("\n"),
  "",
  "## 触控不足（<44×44；≤768 判败，1280 仅记录）明细",
  "",
  findings.length ? findings.map(f => `- ${f.vp} ${f.label} <${f.sel}> \"${f.text}\" h=${f.h} w=${f.w}`).join("\n") : "- （无）",
  "",
  "## 截图 / 错误",
  "",
  "- 截图失败: " + (screenshotWarnings.length ? screenshotWarnings.join("; ") : "无"),
  "- pageerror/console: 见 targeted.log（异常均已列入逐视口）。",
  "- run3 证据保留不动：/tmp/opencode/lit-web-browser/",
  "",
  "## 限制",
  "",
  "- 只复测 run3 三失败项 + 5s 刷新保留抽查，未重复全部 157 项。",
  "- 依赖 builder 最终 web_assets 字节；预检失败即退出码 2（有界即停）。",
].join("\n");
fs.writeFileSync(path.join(RUN_DIR, "report.md"), report);
console.log(`\ntargeted 报告: ${RUN_DIR}/report.md\n日志: ${LOG_DIR}/targeted.log`);
process.exit(totalFail ? 1 : 0);

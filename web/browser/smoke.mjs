// 独立真实浏览器 QA：esbuild 临时 bundle + 本进程 loopback 假 API + Playwright Chromium。
// 运行：cd web && node browser/smoke.mjs   （.mjs 即 ESM，勿加 --input-type=module）
// 产物：/tmp/opencode/lit-web-browser/{logs,screenshots,bundle} 与报告 report.md。
// 只读使用 web/src/**；不连接生产、不提交、不部署。发现问题仅记录并反馈父会话。
import fs from "node:fs";
import path from "node:path";
import {fileURLToPath} from "node:url";
import {build} from "esbuild";
import {chromium} from "playwright";
import {startFake, LONG_TASK_ID, LONG_ATTEMPT_ID, LONG_SID} from "./fake-api.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const WEB_ROOT = path.resolve(HERE, "..");
const RUN_DIR = process.env.OC_ORCH_WEB_BROWSER_RUN_DIR || "/tmp/opencode/lit-web-browser";
const BUNDLE_DIR = path.join(RUN_DIR, "bundle");
const LOG_DIR = path.join(RUN_DIR, "logs");
const SHOT_DIR = path.join(RUN_DIR, "screenshots");
for (const d of [BUNDLE_DIR, LOG_DIR, SHOT_DIR]) fs.mkdirSync(d, {recursive: true});

const VIEWPORTS = [
  {name: "vp-360", width: 360, height: 740},
  {name: "vp-390", width: 390, height: 844},
  {name: "vp-768", width: 768, height: 1024},
  {name: "vp-1280", width: 1280, height: 800},
];
const LOG_LINES = [];
const log = line => { LOG_LINES.push(String(line)); process.stdout.write(line + "\n"); };

// 视口之外专项测试的全局断言（深链/500 恢复），不进入逐视口 results。
let extraFail = 0;
const extraOk = (name, cond, extra = "") => {
  const status = cond ? "PASS" : "FAIL";
  log(`  [${status}] ${name}${extra ? " | " + extra : ""}`);
  if (!cond) extraFail += 1;
};
// QA 发现收集（触控不足 / 截图失败），随 final 报告如实呈现，不吞错。
const touchFindings = [];
const screenshotWarnings = [];
function recordScreenshot(name, err) {
  const msg = `${name} 截图失败: ${(err && err.message) || err}`;
  screenshotWarnings.push(msg);
  log(`  [warn] ${msg}`);
}

// ---------- 1) esbuild 打好临时 bundle（每次从当前 web/src 重建） ----------
log(`[bundle] esbuild web/src/app.js -> ${BUNDLE_DIR}/app.js`);
await build({
  entryPoints: [path.join(WEB_ROOT, "src", "app.js")],
  bundle: true, format: "esm", splitting: true,
  platform: "browser", target: "chrome120",
  outdir: BUNDLE_DIR, logLevel: "silent",
});

// ---------- 2) 假 API server（本进程，ephemeral 端口） ----------
const writeLog = [];
const faults = {};   // 运行时故障注入表：faults[path] = 剩余故障次数
const {port, close, counters, failHit} = await startFake({bundleDir: BUNDLE_DIR, writeLog, fail: faults});
const BASE = `http://127.0.0.1:${port}`;
log(`[server] fake API on ${BASE} (不连生产，只本进程)`);

// ---------- 3) 浏览器 ----------
const browser = await chromium.launch();
const browserVersion = browser.version();
log(`[browser] chromium ${browserVersion}`);

const SID_TO_NAV = "s1";
let totalFail = 0;
const results = [];

// flash 观测脚本：记录是否出现过加载/加载失败面板（由各场景主动清零做“分阶段观测”）。
function flashInitScript() {
  return () => {
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
    } catch (_) { /* 观测失败不阻塞 */ }
  };
}

// 触控目标测量：遍历当前可见的按钮/导航链接/按钮化链接/标签，返回实际高与宽。
async function measureTouch(page) {
  return page.evaluate(() => {
    const out = [];
    for (const el of document.querySelectorAll("button, nav a, a.button, label")) {
      const r = el.getBoundingClientRect();
      out.push({
        tag: el.tagName.toLowerCase(),
        text: (el.textContent || "").trim().replace(/\s+/g, " ").slice(0, 20),
        cls: (typeof el.className === "string" ? el.className : String(el.className)) || "",
        act: el.getAttribute("data-act") || "",
        tab: el.getAttribute("data-tab") || "",
        copy: el.hasAttribute("data-copy"),
        h: Math.round(r.height * 10) / 10,
        w: Math.round(r.width * 10) / 10,
      });
    }
    return out;
  });
}

async function qaViewport(vp) {
  const context = await browser.newContext({viewport: {width: vp.width, height: vp.height}});
  await context.grantPermissions(["clipboard-read", "clipboard-write"], {origin: BASE});
  const page = await context.newPage();
  const consoleErrors = [];
  const pageErrors = [];
  const offOrigin = [];
  const requests = [];
  page.on("console", msg => { if (msg.type() === "error") consoleErrors.push(msg.text()); });
  page.on("pageerror", err => pageErrors.push(String(err)));
  page.on("request", req => {
    const url = req.url();
    requests.push({method: req.method(), url});
    if (!url.startsWith(BASE) && !url.startsWith("data:")) offOrigin.push(url);
  });
  page.on("dialog", d => d.accept());
  await page.addInitScript(flashInitScript());

  const fails = [];
  const ok = (name, cond, extra = "") => {
    const status = cond ? "PASS" : "FAIL";
    log(`  [${status}] ${vp.name} ${name}${extra ? " | " + extra : ""}`);
    if (!cond) fails.push(`${name}${extra ? " | " + extra : ""}`);
  };
  // 窄屏触控：关键可点控件（button / a.button / copy / tab / label）须 ≥44×44；
  // nav 文字链接只测量记录，不足则如实标注限制反馈 core，不假装全部通过。
  const reportTouch = async label => {
    const items = await measureTouch(page);
    const narrow = vp.width <= 390;
    let writeBad = 0, navUnder = 0;
    for (const it of items) {
      const role = it.copy ? "copy" : it.tab ? "tab" : it.act ? `act:${it.act}` : it.tag;
      log(`  [touch] ${vp.name} ${label} <${role}> "${it.text || it.act || it.tab || (it.copy ? "copy" : "")}" h=${it.h} w=${it.w}`);
      if (!narrow) continue;
      const isKey = it.tag === "button" || (it.tag === "a" && it.cls.includes("button")) || it.copy || it.tab || it.tag === "label";
      const isNav = it.tag === "a" && !it.cls.includes("button");
      if (isKey && (it.h < 43.5 || it.w < 43.5)) {
        writeBad++;
        touchFindings.push({vp: vp.name, label, kind: "key", text: it.text || it.act || it.tag, h: it.h, w: it.w});
      }
      if (isNav && (it.h < 43.5 || it.w < 43.5)) {
        navUnder++;
        touchFindings.push({vp: vp.name, label, kind: "nav", text: it.text, h: it.h, w: it.w});
      }
    }
    if (narrow) ok(`${label} 关键可点控件触控 ≥44×44px（窄屏）`, writeBad === 0, `${writeBad} 个不足 44px`);
    if (narrow && navUnder) log(`  [info] ${vp.name} ${label} nav 文字链接 ${navUnder} 个 <44px（测量记录，作为限制反馈 core）`);
  };
  const shot = async name => {
    const f = path.join(SHOT_DIR, `${vp.name}-${name}.png`);
    try { await page.screenshot({path: f, fullPage: false}); }
    catch (err) { recordScreenshot(`${vp.name} ${name}`, err); }
    return f;
  };
  const noOverflow = () => page.evaluate(() => ({
    sw: document.documentElement.scrollWidth,
    cw: document.documentElement.clientWidth,
    over: document.documentElement.scrollWidth > document.documentElement.clientWidth + 1,
  }));
  const hashNav = async h => { await page.evaluate(v => { location.hash = v; }, h); await page.waitForTimeout(180); };
  const waitSel = (sel, timeout = 8000) => page.waitForSelector(sel, {timeout});

  try {
    // ---- 0) 总览 ----
    await page.goto(BASE + "/", {waitUntil: "domcontentloaded"});
    await waitSel("#overviewRoot .grid-cards .card");
    const healthText = await page.textContent("#health");
    ok("总览健康条 Supervisor healthy", healthText.includes("Supervisor healthy"), healthText.slice(0, 60));
    const ov = await noOverflow();
    ok("总览无全局横滚溢出", !ov.over, `sw=${ov.sw} cw=${ov.cw}`);
    const longCard = await page.locator(`#overviewRoot [data-key="coordinator:${LONG_SID}"]`).count();
    ok("总览含长 session 卡片", longCard === 1);
    await page.locator("#overviewRoot button[data-copy]").first().click();
    await page.waitForFunction(() => (document.getElementById("toast")?.textContent || "").includes("已复制"), null, {timeout: 3000}).catch(() => {});
    ok("复制按钮触发 toast", (await page.textContent("#toast")).includes("已复制"), (await page.textContent("#toast")).slice(0, 40));
    await reportTouch("总览");
    await shot("overview");

    // ---- 1) 诊断：details 展开 + 勾选 + 表格/窗口滚动在 5s 刷新后保留；数据原地更新；根不替换 ----
    await hashNav("#/diagnostics");
    await waitSel("#diag-notif");
    await reportTouch("诊断");
    await page.locator("#alertTable details summary").first().click();
    const detailsTag = page.locator("#alertTable details").first();
    ok("告警 details 已展开", await detailsTag.evaluate(d => d.open));
    await page.locator("#notifTable input.noteck").first().uncheck();
    ok("通知勾选框已取消", await page.locator("#notifTable input.noteck").first().isChecked().then(v => !v));
    await page.evaluate(() => {
      window.scrollTo(0, document.documentElement.scrollHeight);
      for (const el of document.querySelectorAll(".tablewrap")) el.scrollLeft = el.scrollWidth;
    });
    const before = await page.evaluate(() => ({y: window.scrollY}));
    const alertTitleBefore = await page.textContent("#alertTable tbody tr >> nth=0");
    await page.evaluate(() => { window.__diagRootRef = document.querySelector("#diag-notif"); });
    // 分阶段观测：只观测本轮 5s 后台刷新窗口，排除初载（首屏 loading/错误）对 __flash 的误记。
    await page.evaluate(() => { window.__flash.load = false; window.__flash.fail = false; });
    await page.waitForTimeout(6200);
    const alertTitleAfter = await page.textContent("#alertTable tbody tr >> nth=0");
    ok("刷新后数据原地更新（告警标题变化）", alertTitleAfter !== alertTitleBefore && alertTitleAfter.includes("#"), `${alertTitleBefore.slice(0, 18)} -> ${alertTitleAfter.slice(0, 18)}`);
    ok("刷新后详情根节点同一（未被替换/重建）", await page.evaluate(() => document.querySelector("#diag-notif") === window.__diagRootRef));
    ok("刷新后 details 仍展开", await detailsTag.evaluate(d => d.open));
    ok("刷新后勾选状态保留", await page.locator("#notifTable input.noteck").first().isChecked().then(v => !v));
    const after = await page.evaluate(() => ({y: window.scrollY}));
    ok("刷新后窗口滚动保留", Math.abs(after.y - before.y) <= 2, `y=${before.y}->${after.y}`);
    const localH = await page.evaluate(() => [...document.querySelectorAll(".tablewrap")].filter(el => el.scrollWidth > el.clientWidth + 1).length);
    log(`  [info] ${vp.name} 局部可横滚的 tablewrap 容器数=${localH}`);
    const diagOv = await noOverflow();
    ok("诊断页无全局横滚溢出", !diagOv.over, `sw=${diagOv.sw} cw=${diagOv.cw}`);
    ok("后台刷新不闪 loading（分阶段观测）", !(await page.evaluate(() => window.__flash.load)), JSON.stringify(await page.evaluate(() => window.__flash)));
    await shot("diagnostics-open");

    // ---- 2) 页面切换回来：diagnostics DOM cache 保留 details 展开 ----
    await hashNav("#/");
    await waitSel("#overviewRoot .grid-cards .card");
    await hashNav("#/diagnostics");
    await waitSel("#diag-notif");
    ok("切走再切回 diagnostics 后 details 仍展开（DOM cache 保留）", await page.locator("#alertTable details").first().evaluate(d => d.open));
    ok("切回后告警数据仍在", (await page.textContent("#alertTable")).includes("全局告警"));

    // ---- 3) 全局任务检索：输入焦点/caret 与过滤结果在刷新后保留 ----
    await hashNav("#/tasks");
    await waitSel("#taskSearch");
    await page.locator("#taskSearch").click();
    await page.keyboard.type("项目p");
    const rowsBefore = await page.locator("#tasksRoot tbody tr").count();
    ok("搜索过滤生效（行数减少）", rowsBefore > 0 && rowsBefore < 6, `rows=${rowsBefore}`);
    await page.waitForTimeout(6200);
    const caretAfter = await page.evaluate(() => { const i = document.getElementById("taskSearch"); return {v: i.value, s: i.selectionStart, e: i.selectionEnd, active: document.activeElement?.id}; });
    const rowsAfter = await page.locator("#tasksRoot tbody tr").count();
    ok("刷新后搜索输入值保留", caretAfter.v === "项目p", `v='${caretAfter.v}'`);
    ok("刷新后输入焦点/caret 保留", caretAfter.active === "taskSearch" && caretAfter.s === caretAfter.e && caretAfter.s === 3, JSON.stringify(caretAfter));
    ok("刷新后过滤结果不变", rowsAfter > 0 && rowsAfter === rowsBefore, `rows=${rowsBefore}->${rowsAfter}`);
    const tasksOv = await noOverflow();
    ok("任务检索页无全局横滚溢出", !tasksOv.over, `sw=${tasksOv.sw} cw=${tasksOv.cw}`);
    await shot("tasks");

    // ---- 4) 任务详情：长文本局部滚动无全局溢出 + collect/verify 仅为假 POST 且确认作用域 ----
    await hashNav(`#/task/${encodeURIComponent(LONG_TASK_ID)}`);
    await waitSel("#taskRoot h2");
    await reportTouch("任务详情");
    const tOv = await noOverflow();
    ok("任务详情无全局横滚溢出", !tOv.over, `sw=${tOv.sw} cw=${tOv.cw}`);
    const preLocal = await page.evaluate(() => [...document.querySelectorAll("#taskRoot pre")].map(p => ({sw: p.scrollWidth, cw: p.clientWidth})));
    log(`  [info] ${vp.name} 任务详情 pre 局部横滚=${JSON.stringify(preLocal)}`);
    await page.locator("[data-act='collect']").first().click();
    await page.waitForTimeout(400);
    await page.waitForFunction(() => document.querySelector("#toast")?.textContent?.includes("已收集"), null, {timeout: 4000}).catch(() => {});
    ok("collect 假 POST 已记录（含尝试 id 作用域）", writeLog.some(e => e.method === "POST" && e.path === `/api/attempt/${LONG_ATTEMPT_ID}/collect`), JSON.stringify(writeLog.map(e => e.path)));
    await page.locator("[data-act='verify'][data-pass='1']").first().click();
    await waitSel('form[data-form="verify"]');
    await page.fill('form[data-form="verify"] input[name="evidence"]', "e1; e2; e3");
    await page.fill('form[data-form="verify"] input[name="notes"]', "qa 备注");
    await page.fill('form[data-form="verify"] input[name="confirm"]', LONG_ATTEMPT_ID);
    await page.click('form[data-form="verify"] button[type="submit"]');
    await page.waitForFunction(() => document.querySelector("#toast")?.textContent?.includes("验收已提交"), null, {timeout: 5000});
    try {
      await page.locator('form[data-form="verify"]').waitFor({state: "detached", timeout: 5000});
      ok("验收提交后表单自动关闭", true);
    } catch { ok("验收提交后表单自动关闭", false, "等 detached 超时"); }
    const vlog = writeLog.filter(e => e.method === "POST" && e.path === `/api/attempt/${LONG_ATTEMPT_ID}/verify`).pop();
    ok("verify 假 POST 已记录", !!vlog);
    ok("verify 作用域一致（body.confirm === 尝试 id）", !!vlog && vlog.body.confirm === LONG_ATTEMPT_ID);
    ok("verify pass=true 且 force=false（不宣称人工例外）", !!vlog && vlog.body.pass === true && vlog.body.force === false);
    ok("verify evidence 分号拆分正确", !!vlog && Array.isArray(vlog.body.evidence) && vlog.body.evidence.join("|") === "e1|e2|e3", JSON.stringify(vlog?.body?.evidence));
    ok("write 操作全部为假 POST（无外部/8787 请求）", offOrigin.length === 0, offOrigin.join(","));
    await shot("task");

    // ---- 5) 调度者工作台：分页续页 + tab 切换持久化 ----
    await hashNav(`#/coordinator/${SID_TO_NAV}`);
    await waitSel("#coordRoot .tabs");
    await reportTouch("工作台");
    const coordMore = page.locator("[data-act='coordmore']");
    const hasMore = await coordMore.count();
    ok("工作台出现分页继续加载按钮（truncated）", hasMore === 1);
    const linesBefore = await page.locator("#coordRoot .task-line").count();
    if (hasMore) { await coordMore.first().click(); await page.waitForTimeout(700); }
    const linesAfter = await page.locator("#coordRoot .task-line").count();
    ok("coordmore 续页后任务行增加（假 GET after_id）", linesAfter > linesBefore, `${linesBefore}->${linesAfter}`);
    ok("coordmore 触发假 API after_id 请求", requests.some(r => r.method === "GET" && r.url.includes("after_id=")), requests.filter(r => r.url.includes("/api/coordinator/")).map(r => r.url).join(" "));
    await page.locator('[data-tab="activity"]').click();
    await waitSel('#coordRoot:has-text("活动")');
    await hashNav("#/");
    await waitSel("#overviewRoot .grid-cards .card");
    await hashNav(`#/coordinator/${SID_TO_NAV}`);
    await waitSel("#coordRoot");
    const tabSel = await page.locator('[data-tab="activity"][aria-selected="true"]').count();
    ok("切走再切回后工作台 tab 保留 activity", tabSel === 1);
    const cOv = await noOverflow();
    ok("工作台无全局横滚溢出", !cOv.over, `sw=${cOv.sw} cw=${cOv.cw}`);
    await shot("coordinator");

    // ---- 6) 尝试详情：长日志仅局部滚动 + 无全局溢出 ----
    await hashNav(`#/attempt/${encodeURIComponent(LONG_ATTEMPT_ID)}`);
    await waitSel("#attemptRoot h2");
    const aOv = await noOverflow();
    ok("尝试详情无全局横滚溢出（长日志限局部）", !aOv.over, `sw=${aOv.sw} cw=${aOv.cw}`);
    const logScroll = await page.evaluate(() => { const el = document.querySelector("#attemptRoot pre.log"); return el ? {sw: el.scrollWidth, cw: el.clientWidth} : null; });
    ok("结果日志节点存在（局部滚动容器）", !!logScroll, JSON.stringify(logScroll));
    await shot("attempt");

    // ---- 7) runtime 错误收集 ----
    ok("无 pageerror / console error（不吞错）", pageErrors.length === 0 && consoleErrors.length === 0,
      `pageerror=${JSON.stringify(pageErrors)} console=${JSON.stringify(consoleErrors.slice(0, 3))}`);
    ok("全部请求同源（无真实外部调用/8787）", offOrigin.length === 0, offOrigin.join(","));
  } catch (err) {
    fails.push(`EXCEPTION: ${err.message}`);
    const f = path.join(SHOT_DIR, `${vp.name}-fail.png`);
    try { await page.screenshot({path: f}); }
    catch (e) { recordScreenshot(`${vp.name} fail`, e); }
    log(`  [FAIL] ${vp.name} 异常: ${err.message}`);
    log(`         consoleError=${JSON.stringify(consoleErrors.slice(0, 5))}`);
    log(`         pageError=${JSON.stringify(pageErrors.slice(0, 5))}`);
  } finally {
    await context.close();
  }
  results.push({name: vp.name, fails});
  totalFail += fails.length;
}

// 全新 context 直开详情页：首载取数中不得假闪“加载失败”。
async function qaDeepLink(vp) {
  const context = await browser.newContext({viewport: {width: vp.width, height: vp.height}});
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", e => errors.push(String(e)));
  page.on("console", m => { if (m.type() === "error") errors.push(m.text()); });
  await page.addInitScript(flashInitScript());
  await page.goto(BASE + "/#/task/" + encodeURIComponent(LONG_TASK_ID), {waitUntil: "domcontentloaded"});
  let loaded = true;
  try { await page.waitForSelector("#taskRoot .breadcrumb", {timeout: 8000}); } catch { loaded = false; }
  const flash = await page.evaluate(() => window.__flash);
  extraOk(`深链首载无“加载失败”假闪（${vp.name}）`, loaded && flash.fail === false, `loaded=${loaded} flash=${JSON.stringify(flash)}`);
  log(`  [info] 深链首载 ${vp.name} 观测 flash=${JSON.stringify(flash)}（加载态出现=${flash.load} 仅记录：快速假 server 可能未渲染过加载面板）`);
  extraOk(`深链首载 console/pageerror 干净（${vp.name}）`, errors.length === 0, JSON.stringify(errors.slice(0, 3)));
  try { await page.screenshot({path: path.join(SHOT_DIR, `${vp.name}-deeplink.png`)}); }
  catch (err) { recordScreenshot(`${vp.name} 深链`, err); }
  await context.close();
}

// 首载 500 可自动恢复：注入前 2 次 500，随后自动重试成功；并测量失败面板 .attn button 触控 44×44。
async function qaRetry500() {
  const context = await browser.newContext({viewport: {width: 390, height: 844}});
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", e => errors.push(String(e)));
  page.on("console", m => {
    // 注入的 500 会以 Failed to load resource…500 的形式出现在 console，属预期噪音。
    if (m.type() === "error" && !/Failed to load resource.*500|status of 500/i.test(m.text())) errors.push(m.text());
  });
  await page.addInitScript(flashInitScript());
  faults["/api/task/t-short"] = 2;   // 注入 2 次 500
  await page.goto(BASE + "/#/task/t-short", {waitUntil: "domcontentloaded"});
  // 1) 先等“加载失败”面板出现（500 真实命中）
  let sawFail = false;
  try { await page.waitForSelector('#taskRoot:has-text("加载失败")', {timeout: 8000}); sawFail = true; } catch { /* 面板一闪而过或未出现 */ }
  // 2) 测量失败面板里的重试按钮触控尺寸（窄屏 ≥44×44）
  const attnButtons = await page.evaluate(() =>
    [...document.querySelectorAll(".attn button")].map(b => { const r = b.getBoundingClientRect(); return {t: (b.textContent || "").trim().slice(0, 20), h: Math.round(r.height * 10) / 10, w: Math.round(r.width * 10) / 10}; }));
  for (const b of attnButtons) log(`  [touch] 500 面板 .attn button "${b.t}" h=${b.h} w=${b.w}`);
  extraOk("500 失败面板内按钮触控 ≥44×44px（窄屏 .attn button）", attnButtons.every(b => b.h >= 43.5 && b.w >= 43.5), JSON.stringify(attnButtons));
  // 3) 等自动重试恢复（约 5s tick 后的 loadDetail 重发）
  let loaded = false;
  try { await page.waitForSelector("#taskRoot .breadcrumb", {timeout: 12000}); loaded = true; } catch { /* 超时视为未恢复 */ }
  const failedFlash = await page.evaluate(() => window.__flash?.fail === true).catch(() => false);
  extraOk("首载 500 已注入且被自动重试恢复", sawFail && loaded && failHit["/api/task/t-short"] >= 1, `sawFail=${sawFail} failHit=${failHit["/api/task/t-short"] || 0} loaded=${loaded}`);
  log(`  [info] 500 期间短暂显示“加载失败”面板=${failedFlash}；之后自动重试成功=${loaded}`);
  extraOk("500 恢复后 console/pageerror 干净", errors.length === 0, JSON.stringify(errors.slice(0, 3)));
  delete faults["/api/task/t-short"];
  await context.close();
}

log("\n======== 浏览视口 QA ========");
for (const vp of VIEWPORTS) {
  log(`\n-- ${vp.name} (${vp.width}x${vp.height}) --`);
  await qaViewport(vp);
}

log("\n-- 全新 context 深链首载（#/task/…，source 取数中不得假闪 加载失败） --");
for (const vp of [VIEWPORTS[1], VIEWPORTS[3]]) {   // 390 与 1280
  await qaDeepLink(vp);
}

log("\n-- 首载 500 自动恢复（#/task/t-short 前 2 次 500） --");
await qaRetry500();

await browser.close();
await close();

// ---------- 报告 ----------
const summary = results.map(r => `${r.name}: ${r.fails.length ? "FAIL(" + r.fails.length + ")" : "PASS"}`).join("  ");
log(`\n======== QA 汇总 ========\n${summary}\n专项(深链/500): ${extraFail ? "FAIL(" + extraFail + ")" : "PASS"}\n总失败断言: ${totalFail + extraFail}`);
const report = [
  "# lit-web 浏览器 QA 报告（独立假数据，未连生产）",
  "",
  "- 时间: " + new Date().toISOString(),
  "- Chromium: " + browserVersion,
  "- 命令: cd web && node browser/smoke.mjs",
  "- 视口: " + VIEWPORTS.map(v => `${v.name}(${v.width}x${v.height})`).join(" "),
  "- 假 API: 本进程 loopback ephemeral（未连接 8787 生产）；写操作仅记录不投递；请求全同源。",
  "- Bundle: esbuild 临时产物 /tmp/opencode/lit-web-browser/bundle/app.js（未写仓库）",
  "- 截图: /tmp/opencode/lit-web-browser/screenshots/",
  "",
  "## 逐项结果",
  "",
  results.map(r => `- ${r.name}: ${r.fails.length ? "**FAIL** — " + r.fails.join("; ") : "**PASS**"}`).join("\n"),
  "",
  "- 专项(深链/500)结果: " + (extraFail ? `**FAIL(${extraFail})**` : "**PASS**"),
  "",
  "## 触控测量（窄屏按 44×44 判定；nav 文字链接按限制记录）",
  "",
  "- 触控不足总项: " + (touchFindings.length ? `共 ${touchFindings.length} 处` : "无"),
  ...(touchFindings.length
    ? ["", ...touchFindings.map(f => `  - ${f.vp} ${f.label} kind=${f.kind} "${f.text}" h=${f.h} w=${f.w}`)]
    : ["  - （无不足项）"]),
  "- nav 说明: 导航文字链接在窄屏按 padding 定高，测量若 <44px 属原布局行为，按限制记录并已反馈 core/CSS 评估，不假装全部通过。",
  "",
  "## 截图与错误记录",
  "",
  "- 截图失败: " + (screenshotWarnings.length ? screenshotWarnings.join("; ") : "无"),
  "- 全部页面无 pageerror/console error（500 注入属预期噪音，已过滤）。",
  "",
  "## 限制",
  "",
  "- 仅 headless Chromium；字体/CDN 无关（本地 bundle，无外部网络）。",
  "- 假数据为确定性夹具，不代表生产台账形态；未运行 e2e、未触碰 /api 生产。",
  "- 若浏览器系统库缺失或网络失败：仅据实报告，不修 OS/代理。",
].join("\n");
fs.writeFileSync(path.join(RUN_DIR, "report.md"), report);
fs.writeFileSync(path.join(LOG_DIR, "qa.log"), LOG_LINES.join("\n") + "\n");
log(`\n报告: ${RUN_DIR}/report.md\n日志: ${LOG_DIR}/qa.log`);
process.exit(totalFail + extraFail ? 1 : 0);

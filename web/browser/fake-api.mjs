// 假 API 夹具 + 同源 loopback 静态/API server（QA 专用，不连生产）。
// 只在本进程内提供确定性数据；写操作只记录（writeLog），不投递、不改账。
import http from "node:http";
import fs from "node:fs";
import path from "node:path";

export const LONG_SID = "ses_" + "a".repeat(54);            // 60 字符 session id
export const LONG_TASK_ID = "task-" + "x".repeat(60);       // 65 字符 task id
export const LONG_ATTEMPT_ID = "att-" + "y".repeat(58);     // 62 字符 attempt id

const SID1 = "s1";
const TASK_S = {id: "t-short", title: "短标题任务", status: "running", project: "项目p"};

function longText(seed, n = 300) {
  const base = "很长很长很长很长很长很长很长很长很长很长很长很长很长很长很长的内容";
  return (base.repeat(Math.ceil(n / base.length)) + seed).slice(0, n);
}

export function makeFixtures() {
  const snapshotBase = {
    read_only: false,
    supervisor: {healthy: true, pid: "4242", connected: true},
    service: {base: "loopback-fake", version: "9.9.9-qa"},
    counts: {tasks: 6},
    tasks: [
      {id: TASK_S.id, title: "总览速查任务", status: "running", project: "项目p", coordinator_session: SID1, last_progress_at: "2026-11-12T00:10:00Z"},
      {id: LONG_TASK_ID, title: longText("长标题任务", 140), status: "blocked", project: "非常长的项目名字用于测试横滚".repeat(3), coordinator_session: LONG_SID, blocked_reason: "等待外部输入确认", last_progress_at: null},
      {id: "t-a1", title: "已验收任务", status: "accepted", project: "项目q", coordinator_session: null, last_progress_at: "2026-11-12T00:05:00Z"},
      {id: "t-c1", title: "已取消任务", status: "cancelled", project: "项目r", coordinator_session: SID1, last_progress_at: "2026-11-12T00:01:00Z"},
      {id: "t-x2", title: "运行中任务二", status: "running", project: "项目p", coordinator_session: SID1, last_progress_at: "2026-11-12T00:09:00Z"},
      {id: "t-x3", title: "待验收任务", status: "running", project: "项目p", coordinator_session: SID1, last_progress_at: "2026-11-12T00:08:00Z"},
    ],
    leases: [
      {project: "项目p", session: SID1, expires_at: "2026-11-13T00:00:00Z", valid: true, recovery_blocked: false, failure_count: 0},
    ],
    provider_backoffs: {"ProviderZ": {until: "2026-11-12T23:59:59Z", reason: "冷却"}},
    target_gates: [{project: "项目p", state: "cooling"}],
    notifications: [
      {id: "n-" + "0".repeat(50) + "1", kind: "task", state: "pending", target_session: SID1, task_id: TASK_S.id, attempt_id: LONG_ATTEMPT_ID, summary: longText("待ACK通知摘要", 200), missing_deliverables: ["out/result.json"], last_error: ""},
      {id: "n-2", kind: "lease", state: "sent", target_session: SID1, task_id: "", attempt_id: "", summary: "租约到期提醒", last_error: ""},
    ],
  };

  return {
    snapshotBase,
    coordinators: {
      s1: {
        session: SID1,
        title: "Alpha 调度者（协调任务/租约证据确认）",
        identity: "confirmed",
        sources: ["task:t-short", "lease:项目p", "notify:n-1"],
        created_at: "2026-11-01T00:00:00Z", created_at_known: true, created_meta_stale: false, metadata_fresh: true,
        online: true,
        projects: ["项目p"],
        leases: [{project: "项目p", valid: true, recovery_blocked: false, failure_count: 0, expires_at: "2026-11-13T00:00:00Z"}],
        gates: [{state: "manual-blocked", source: "manual", reason: "等待人工验证验收标准"}],
        counts: {unfinished: 4, awaiting_verification: 2, blocked: 1, accepted: 1, cancelled: 1, notifications_pending: 1},
        last_update: "2026-11-12T00:10:00Z",
        attention: "有任务受阻且存在手动闸门", attention_rank: 1,
        task_tips: [
          {id: TASK_S.id, title: "总览速查任务", status: "running"},
          {id: "t-x2", title: "运行中任务二", status: "running"},
          {id: "t-x3", title: "待验收任务", status: "running"},
        ],
        task_tips_truncated: true,
      },
      [LONG_SID]: {   // 长 id session：仅 notify 关联、元数据过期
        session: LONG_SID,
        title: "Long Session Title ".repeat(3).trim(),
        identity: null,
        sources: ["notify:old"],
        created_at: null, created_at_known: false, created_meta_stale: true, metadata_fresh: false,
        online: true,
        projects: [], leases: [], gates: [],
        counts: {},
        last_update: null,
        attention: "", attention_rank: 9,
        task_tips: [], task_tips_truncated: false,
      },
    },
    coordMeta: {
      unbound_task_count: 12,
      metadata_fresh: true,
      task_counts_note: "全量台账计数 12 项未绑定（示例）",
      created_at_known: 1,
      created_at_unknown: 1,
    },
    // coordinator 任务池：35 项，第一页 25（截断），续页返回剩余。
    coordTaskPool: Array.from({length: 35}, (_, i) => {
      const n = String(i).padStart(3, "0");
      return {
        id: `coord-t-${n}`,
        title: `工作台任务 ${n}：${longText("", 60)}`,
        status: i < 4 ? "running" : i === 4 ? "blocked" : i === 5 ? "accepted" : "cancelled",
        owned_via: i % 2 ? "explicit" : "lease",
        project: i % 2 ? "项目p" : "项目q",
        current_attempt: i < 4 ? `att-${n}` : null,
        blocked_reason: i === 4 ? "闸门阻塞原因说明" : "",
        updated_at: `2026-11-12T00:${String(i % 60).padStart(2, "0")}:00Z`,
      };
    }),
    coordNotifications: [
      {id: "cn-1", kind: "task", state: "pending", target_session: SID1, task_id: TASK_S.id, attempt_id: "", summary: "投递回执待 ACK", sent_at: "2026-11-12T00:02:00Z"},
      {id: "cn-2", kind: "lease", state: "sent", target_session: SID1, task_id: "", attempt_id: "", summary: "已发送租约通知", sent_at: "2026-11-12T00:03:00Z"},
    ],
    coordResp: [
      {id: "r-1", owner: SID1, target_session: "ses_worker_1", state: "pending", wait_kind: "result", wait_condition: "等待尝试 xxxx 产出结果", next_action: "collect", next_check_at: "2026-11-12T00:20:00Z", deadline: "2026-11-12T01:00:00Z", task_id: TASK_S.id, attempt_id: LONG_ATTEMPT_ID, generation: 1, result_version: 0, budget_used: 2, max_budget: 10, revision: 3},
      {id: "r-2", owner: "ses_worker_1", target_session: SID1, state: "blocked", wait_kind: "verify", wait_condition: "等待验收结论", next_action: "verify", next_check_at: null, deadline: null, task_id: "t-x3", attempt_id: "att-003", generation: 2, result_version: 1, budget_used: 1, max_budget: 5, revision: 1},
    ],
  };
}

export function startFake({bundleDir, writeLog = [], fail = {}, assetsDir}) {
  const fixtures = makeFixtures();
  let statusCount = 0;
  const failHit = {};   // 记录每次注入的命中次数（供断言“确实发生过故障”）
  const counters = {status: 0, coord: 0, task: 0, attempt: 0, unbound: 0, extras: 0, post: 0};
  const snap = () => {
    statusCount += 1;
    return {
      ...fixtures.snapshotBase,
      alerts: [
        {id: "al-1", severity: "high", kind: "timeout", title: `全局告警 #${statusCount}`, task_id: null, attempt_id: null, detail: longText("<告警原始正文> " + "L".repeat(400), 500)},
        {id: "al-2", severity: "low", kind: "lease", title: "租约告警", task_id: TASK_S.id, attempt_id: LONG_ATTEMPT_ID, detail: "租约接近到期"},
        {id: "al-3", severity: "mid", kind: "dispatch", title: "派单重试告警", task_id: "", attempt_id: "", detail: "dispatch 连续失败 3 次，等待退避"},
      ],
      notifications: fixtures.snapshotBase.notifications,
    };
  };
  const taskPools = {
    page1: fixtures.coordTaskPool.slice(0, 25),
    page2: fixtures.coordTaskPool.slice(25),
  };
  const unboundPage1 = {
    tasks: [
      {id: "ub-" + "0".repeat(50) + "1", title: longText("未绑定长标题一", 130), status: "blocked", project: "", blocked_reason: "无协调者且无租约", updated_at: "2026-11-12T00:00:00Z"},
      {id: "ub-2", title: "未绑定任务二", status: "running", project: "无主项目", blocked_reason: "", updated_at: "2026-11-12T00:01:00Z"},
      {id: "ub-3", title: "未绑定任务三", status: "pending", project: "项目z", blocked_reason: "", updated_at: null},
    ],
    full_count: 42,
    next_after_id: "ub-3",
    truncated: true,
  };

  const send = (res, code, obj, type = "application/json; charset=utf-8") => {
    res.writeHead(code, {"Content-Type": type, "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"});
    res.end(type.includes("json") ? JSON.stringify(obj) : obj);
  };
  const readBody = req => new Promise(resolve => {
    let data = "";
    req.on("data", c => { data += c; });
    req.on("end", () => {
      try { resolve(data ? JSON.parse(data) : {}); }
      catch { resolve({}); }
    });
    req.on("error", () => resolve({}));
  });

  const server = http.createServer(async (req, res) => {
    const u = new URL(req.url, "http://127.0.0.1");
    const m = req.method.toUpperCase();
    const p = u.pathname;
    try {
      if (p === "/favicon.ico") { res.writeHead(204); res.end(); return; }
      // 静态资源：优先使用外部最终 package 字节（assetsDir，如 web_assets），否则回退 esbuild 临时 bundle + 源码 CSS/index。
      if (p === "/assets/app.js") {
        const f = assetsDir ? path.join(assetsDir, "app.js") : path.join(bundleDir, "app.js");
        return send(res, 200, fs.readFileSync(f), "text/javascript; charset=utf-8");
      }
      if (p === "/assets/app.css") {
        const f = assetsDir ? path.join(assetsDir, "app.css") : new URL("../src/styles.css", import.meta.url);
        return send(res, 200, fs.readFileSync(f), "text/css; charset=utf-8");
      }
      if (p === "/") {
        const f = assetsDir ? path.join(assetsDir, "index.html") : new URL("../index.html", import.meta.url);
        const html = fs.readFileSync(f, "utf-8");
        return send(res, 200, html, "text/html; charset=utf-8");
      }
      if (p.startsWith("/api/")) {
        // 测试注入故障：fail[path] 为剩余故障次数（可运行时改动），命中返回 500，模拟首载临时故障（可被自动重试恢复）。
        const remaining = fail[p];
        if (remaining > 0) {
          fail[p] = remaining - 1;
          failHit[p] = (failHit[p] || 0) + 1;
          return send(res, 500, {message: "服务器临时故障（测试注入）", code: "SERVER_ERROR"});
        }
        if (m !== "GET") {
          const body = m === "POST" ? await readBody(req) : {};
          counters.post += 1;
          writeLog.push({method: m, path: p, query: u.search, body, at: Date.now()});
          if (p === "/api/coordinator/unlock") return send(res, 200, {ok: true, result: {cleared: true, resurfaced: 1}});
          return send(res, 200, {ok: true});
        }
        if (p === "/api/status") { counters.status += 1; return send(res, 200, {snapshot: snap()}); }
        if (p === "/api/coordinators") {
          counters.coord += 1;
          return send(res, 200, {coordinators: [fixtures.coordinators.s1, fixtures.coordinators[LONG_SID]], meta: fixtures.coordMeta});
        }
        if (p === "/api/audit") return send(res, 200, {
          audit: [
            {id: "aud-1", at: "2026-11-12T00:10:00Z", actor: "web", action: "verify", entity: "attempt", entity_id: LONG_ATTEMPT_ID, from_state: "result_pending", to_state: "accepted"},
            {id: "aud-2", at: "2026-11-12T00:09:59Z", actor: "web", action: "cancel", entity: "task", entity_id: TASK_S.id, from_state: "running", to_state: "cancelled"},
            {at: "2026-11-12T00:09:00Z", actor: "svc", action: "collect", entity: "attempt", entity_id: "att-miss", from_state: "", to_state: "collected"},
          ],
        });
        if (p === "/api/inputs") return send(res, 200, {inputs: [
          {input_id: "inp-" + "5".repeat(40), message_id: "msg-" + "6".repeat(40), session_id: SID1, owner: "web", intent: "execute", admission: "admitted", delivery: "delivered", execution: "succeeded", business_ack: "unacknowledged", next_action: "ack", pending_age_ms: 90000, revision: 4},
          {input_id: "inp-2", message_id: "msg-2", session_id: "ses_worker_1", owner: "web", intent: "park", admission: "admitted", delivery: "prepared", execution: "pending", business_ack: "acknowledged", next_action: "watch", pending_age_ms: 3600_000, revision: 1},
        ]});
        if (p === "/api/input-watches") return send(res, 200, {watches: [
          {session_id: SID1, owner: "web", project: "项目p", enabled: true, max_pending_age_ms: 300000, revision: 4, pending_count: 2, next_action: "ack", last_observed_at: "2026-11-12T00:10:00Z", last_observation: {pending: 2, oldest_age_ms: 90000, trusted: true, aged_out: false, stale: false, blocking_reason: null, next_action: "ack"}},
        ]});
        if (p === "/api/responsibilities") return send(res, 200, {responsibilities: fixtures.coordResp.slice(0, 2)});
        // R2：业务健康与本地人工待办（前端每轮刷新必取；必须给良性响应避免 console 404）
        if (p === "/api/health") {
          counters.health = (counters.health || 0) + 1;
          return send(res, 200, {
            ok: true, schema: "oc-orch.continuity-health.v1", scanned: 0,
            process: {healthy: true, connected: true, heartbeat_age_s: 0,
                      last_cycle_at: Date.now()},
            semantics: "process-health-not-business-acceptance",
            tasks: [], events: [],
            coverage: {complete: true, next_after_id: null, cursor_advanced: false,
                       scope: {project: null, session_id: u.searchParams.get("session_id") || null}},
            runtime: {package_version: "0.1.0", schema_version: 3, release_id: null,
                      manifest_verified: false, source: "development-or-unmanifested",
                      native_trusted_chat_enabled: false, email_default_enabled: false},
          });
        }
        if (p === "/api/incidents") return send(res, 200, {
          incidents: [], status: {total: 0, by_state: {}, by_type: {}, recipient_refs: [],
                                  channel: "not-configured", email_configured: false,
                                  email_policy_enabled: false},
        });
        if (p === "/api/tasks/unbound") {
          counters.unbound += 1;
          return send(res, 200, unboundPage1);
        }
        const cm = p.match(/^\/api\/coordinator\/([^/]+)$/);
        if (cm) {
          counters.coord += 1;
          const page = u.searchParams.get("after_id") ? taskPools.page2 : taskPools.page1;
          const coord = cm[1] === fixtures.coordinators.s1.session ? fixtures.coordinators.s1
            : fixtures.coordinators[cm[1]] || fixtures.coordinators.s1;
          return send(res, 200, {
            coordinator: coord,
            tasks: page,
            tasks_full_count: 35,
            tasks_truncated: !u.searchParams.get("after_id"),
            next_after_id: u.searchParams.get("after_id") ? null : "coord-t-025",
            tasks_scope: "示例：按任务 ID keyset 升序（分页为假数据）",
            notifications: fixtures.coordNotifications,
            responsibilities: fixtures.coordResp,
          });
        }
        const tm = p.match(/^\/api\/task\/([^/]+)$/);
        if (tm) {
          counters.task += 1;
          return send(res, 200, {
            task: {
              id: tm[1],
              title: tm[1] === TASK_S.id ? "总览速查任务" : longText("长标题任务详情", 160),
              status: "running",
              project: "项目p",
              effective_coordinator: SID1,
              coordinator_source: "explicit",
              created_at: "2026-11-01T00:00:00Z",
              updated_at: "2026-11-12T00:10:00Z",
              accepted_at: null, closed_at: null,
              deadline_at: "2026-11-12T23:59:59Z",
              next_check_at: "2026-11-12T00:20:00Z",
              blocked_reason: "",
              objective: longText("目标：交付可复现的构建产物并附验收证据链。", 400),
              acceptance: "验收标准：构建通过、产物存在、证据包含 checksum 且可复现；未达到时给出 unmet 清单。",
              current_attempt_id: LONG_ATTEMPT_ID,
            },
            attempts: [
              {
                id: LONG_ATTEMPT_ID, generation: 3, status: "result_pending", dispatch_state: "dispatched",
                session_id: "ses_worker_worker_worker_worker_0001", created_at: "2026-11-12T00:06:00Z",
                started_at: "2026-11-12T00:06:05Z", last_progress_at: "2026-11-12T00:09:00Z",
                last_observed_at: null, dispatched_at: "2026-11-12T00:06:01Z",
                result_path: "/tmp/out/" + "d".repeat(50) + "/result.json",
                verification: {},
              },
              {id: "att-002", generation: 2, status: "accepted", dispatch_state: "acknowledged", session_id: "ses_worker_1", result_path: "/tmp/out/v2.json", verification: {result: "pass", classification: "evidence", verified_at: "2026-11-11T10:00:00Z", actor: "web"}},
              {id: "att-001", generation: 1, status: "cancelled", dispatch_state: "cancelled", session_id: "ses_worker_1", result_path: "", verification: {result: "fail", classification: "rework", verified_at: "2026-11-10T09:00:00Z", actor: "web", reason: "产物缺失"}},
            ],
            attempts_full_count: 4,
            attempts_truncated: true,
          });
        }
        const am = p.match(/^\/api\/attempt\/([^/]+)$/);
        if (am) {
          counters.attempt += 1;
          return send(res, 200, {
            attempt: {
              id: am[1], task_id: TASK_S.id, generation: 3, status: "result_received",
              dispatch_state: "acknowledged", dispatch_msg_id: "msg-" + "7".repeat(40),
              session_id: "ses_worker_worker_worker_worker_0001", directory: "/tmp/workspace/" + "w".repeat(40),
              created_at: "2026-11-12T00:06:00Z", dispatched_at: "2026-11-12T00:06:01Z", started_at: "2026-11-12T00:06:05Z",
              last_progress_at: "2026-11-12T00:09:00Z", last_observed_at: "2026-11-12T00:09:30Z",
              stop_requested_at: null, stop_confirmed_at: null, site_released_at: null,
              result_collected_at: null, terminal_at: null, terminal_kind: null, terminal_reason: "",
              result_path: "/tmp/out/" + "d".repeat(50) + "/result.txt",
              result_summary: longText("日志第一行：", 60) + "\n" + longText("L1-" + "0".repeat(300), 300) + "\n" + longText("L2-日志含很长很长的不可换行文本用于验证局部横滚是否被正确收敛在 pre 内部而不是撑破页面布局边界造成全局溢出。", 200),
              verification: {result: "", classification: "", verified_at: null, actor: "", reason: "", force_reason: "", missing: [], evidence: []},
            },
            task: {id: TASK_S.id, title: "总览速查任务", status: "running"},
            deliverables: [
              {target: "out/" + "f".repeat(50) + ".json", kind: "file", status: "delivered", checked_at: "2026-11-12T00:09:00Z", evidence: ["sha256:" + "e".repeat(64)]},
              {target: "out/report.md", kind: "file", status: "checking", checked_at: null, evidence: []},
            ],
          });
        }
        return send(res, 404, {message: "not found", code: "NOT_FOUND"});
      }
      return send(res, 404, {message: "not found", code: "NOT_FOUND"});
    } catch (err) {
      send(res, 500, {message: String((err && err.message) || err), code: "SERVER_ERROR"});
    }
  });
  return new Promise(resolve => {
    server.listen(0, "127.0.0.1", () => {
      resolve({port: server.address().port, close: () => new Promise(r => server.close(r)), counters, failHit});
    });
  });
}
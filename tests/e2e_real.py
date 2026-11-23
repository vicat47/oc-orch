#!/usr/bin/env python3
"""oc-orch 真实接线验收。

- 使用真实 OpenCode v2.2.5 API 与真实 session；不修改共享服务。
- 每个场景使用独立 OC_ORCH_HOME，避免污染生产台账。
- 故障注入通过本机 TCP 代理隔离，不重启共享服务。
- 合成/单元覆盖见 tests/test_unit.py；本文件只标注真实/代理注入。

用法：
  python3 tests/e2e_real.py --only s1,s2
  python3 tests/e2e_real.py --all
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import traceback
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "src"))

from oc_orch.client import Client, load_service  # noqa: E402
from oc_orch.util import now_ms  # noqa: E402

CLI = os.path.join(REPO, "bin", "oc-orch")
SERVICE = None  # 仅在 main 显式选择真实场景后读取服务凭据。
MODEL = "opencode-go/deepseek-v4.1-flash"


class CheckFail(Exception):
    pass


def check(name: str, cond: bool, detail=None):
    if not cond:
        raise CheckFail(f"{name}: {detail}")
    print(f"  PASS {name}" + (f" :: {detail}" if detail else ""))


class H:
    def __init__(self, root: str):
        self.root = root
        self.run_stamp = time.strftime("%Y%m%dT%H%M%S")
        self.base = os.path.join(root, f"run-{self.run_stamp}")
        os.makedirs(self.base, mode=0o700, exist_ok=True)
        self.real = Client(service=SERVICE)
        self.created_sessions: list[str] = []
        self.supervisors: list[subprocess.Popen] = []
        self.daemon_homes: list[str] = []
        self.homes: list[str] = []
        self.proxies: list[subprocess.Popen] = []
        self.evidence = {"schema": "oc-orch.e2e.v1", "started_at": now_ms(),
                         "service": SERVICE.public(), "scenarios": [],
                         "model": MODEL, "python": sys.version,
                         "code_base": subprocess.check_output(
                             ["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
                         "source_sha256": {
                             str(p.relative_to(REPO)): hashlib.sha256(p.read_bytes()).hexdigest()
                             for p in sorted([*Path(REPO, "src").rglob("*.py"),
                                              Path(__file__).resolve(), Path(HERE, "fault_proxy.py")])}}
        self.scenario = None

    # ---- 基础 -----------------------------------------------------
    def home(self, name: str) -> str:
        path = os.path.join(self.base, name)
        os.makedirs(path, mode=0o700, exist_ok=True)
        self.homes.append(path)
        with open(os.path.join(path, "config.json"), "w", encoding="utf-8") as f:
            json.dump({
                "reconcile_interval": 2,
                "heartbeat_interval": 1,
                "ack_timeout": 90,
                "notification_retry_min": 2,
                "notification_retry_max": 10,
                "recovery_delay": 1,
                "stall_after": 1200,
                "tool_expected_grace": 120,
                "auto_collect": True,
                "desktop_alerts": False,
                "max_wake_attempts": 3,
            }, f)
        return path

    def env(self, home: str, service_config: str | None = None) -> dict:
        e = os.environ.copy()
        e["OC_ORCH_HOME"] = home
        if service_config:
            e["OC_ORCH_SERVICE_CONFIG"] = service_config
        return e

    def cli(self, home: str, *args, service_config: str | None = None,
            check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess:
        cmd = [sys.executable, CLI, "--home", home, "--json", *args]
        p = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout,
                            env=self.env(home, service_config))
        if args and args[0] in ("dispatch", "rework"):
            try:
                result = json.loads(p.stdout)
                sid = result.get("session_id") or (result.get("detail") or {}).get("session_id")
                if sid and sid not in self.created_sessions:
                    self.created_sessions.append(sid)
            except (ValueError, TypeError):
                pass
        if check and p.returncode != 0:
            raise CheckFail(f"CLI {' '.join(args)} exit={p.returncode}\n"
                            f"stdout={p.stdout}\nstderr={p.stderr}")
        return p

    def cli_json(self, home: str, *args, **kw) -> dict:
        p = self.cli(home, *args, **kw)
        try:
            return json.loads(p.stdout)
        except ValueError:
            raise CheckFail(f"CLI 非 JSON 输出：{p.stdout[:800]}")

    def wait(self, fn, timeout: float, interval: float = 1.0, what: str = "condition"):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                v = fn()
                if v:
                    return v
                last = v
            except Exception as e:
                last = repr(e)
            time.sleep(interval)
        raise CheckFail(f"等待超时 {timeout}s: {what}; last={last}")

    # ---- 真实 API -------------------------------------------------
    def session(self, title: str, directory: str, *, model: str | None = None,
                permissions=None) -> dict:
        model = model or MODEL
        provider, mid = model.split("/", 1)
        body = {
            "title": title,
            "location": {"directory": directory},
            "agent": "build",
            "model": {"providerID": provider, "id": mid},
            "metadata": {"oc_orch_e2e": True},
        }
        if permissions is not None:
            body["permissions"] = permissions
        planned = "ses_ocorch" + uuid.uuid4().hex[:16]
        self.created_sessions.append(planned)
        out = self.real.create_session(
            session_id=planned,
            directory=directory, title=title, agent="build",
            model=body["model"], metadata=body["metadata"], permissions=permissions,
        )
        return out

    def get_session(self, sid: str) -> dict | None:
        return self.real.get_session(sid)

    def wait_outcome(self, sid: str, timeout: float = 300) -> dict:
        def f():
            s = self.real.get_session(sid)
            return s if s and s.get("outcome") else None
        return self.wait(f, timeout, 2, f"session {sid} outcome")

    def messages(self, sid: str, **kw) -> list[dict]:
        return self.real.messages(sid, **kw)

    def user_message_ids(self, sid: str, limit: int = 100) -> list[str]:
        return [m["id"] for m in self.messages(sid, limit=limit, order="desc")
                if m.get("type") == "user"]

    # ---- 台账 -----------------------------------------------------
    def db(self, home: str) -> sqlite3.Connection:
        conn = sqlite3.connect(os.path.join(home, "ledger.db"))
        conn.row_factory = sqlite3.Row
        return conn

    def rows(self, home: str, sql: str, args=()) -> list[dict]:
        conn = self.db(home)
        try:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]
        finally:
            conn.close()

    def task(self, home: str, task_id: str) -> dict:
        return self.cli_json(home, "task", "show", task_id)["task"]

    def attempt(self, home: str, attempt_id: str) -> dict:
        return self.cli_json(home, "attempt", "show", attempt_id)["attempt"]

    def notifs(self, home: str, where: str = "1=1", args=()) -> list[dict]:
        return self.rows(home, f"SELECT * FROM notifications WHERE {where} "
                               "ORDER BY created_at", args)

    # ---- 派单 -----------------------------------------------------
    def task_create(self, home: str, title: str, directory: str, coordinator: str,
                    deliverables=(), project: str = "e2e", **kw) -> str:
        args = ["task", "create", "--project", project, "--title", title,
                "--objective", title, "--coordinator", coordinator,
                "--scope", directory]
        for d in deliverables:
            args += ["--deliverable", d]
        for k, v in kw.items():
            if v is not None:
                args += [f"--{k.replace('_', '-')}", str(v)]
        return self.cli_json(home, *args)["task"]["id"]

    def dispatch(self, home: str, task_id: str, directory: str, prompt: str,
                 *, session: str | None = None, service_config: str | None = None,
                 stall_after: int | None = None, model: str | None = None,
                 coordinator: str | None = None, check: bool = True,
                 no_prompt: bool = False, defer_unknown: bool = False) -> dict:
        args = ["dispatch", "--task", task_id, "--directory", directory,
                "--agent", "build"]
        if not no_prompt:
            args += ["--model", model or MODEL, "--prompt", prompt]
        if session:
            args += ["--session", session]
        else:
            args += ["--new-session"]
        if no_prompt:
            args += ["--no-prompt"]
        if stall_after:
            args += ["--stall-after", str(stall_after)]
        if coordinator:
            args += ["--coordinator", coordinator]
        p = self.cli(home, *args, service_config=service_config, check=False,
                     timeout=150)
        if p.returncode == 6:
            # 请求已受理但响应丢失/超时：保留原 ID，先让 Supervisor 对账
            try:
                detail = json.loads(p.stdout)["detail"]
            except Exception:
                detail = {}
            if not defer_unknown:
                self.supervisor_once(home, service_config=service_config)
            return {"ok": True, "dispatch_unknown": True,
                    "attempt_id": detail.get("attempt_id"),
                    "session_id": detail.get("session_id"),
                    "message_id": detail.get("message_id"),
                    "exit": 6, "stdout": p.stdout, "stderr": p.stderr}
        if check and p.returncode != 0:
            raise CheckFail(f"CLI dispatch exit={p.returncode}\n"
                            f"stdout={p.stdout}\nstderr={p.stderr}")
        if p.returncode != 0:
            return {"ok": False, "exit": p.returncode,
                    "stdout": p.stdout, "stderr": p.stderr}
        return json.loads(p.stdout)

    def supervisor_once(self, home: str, service_config: str | None = None) -> dict:
        return self.cli_json(home, "reconcile", "--dispatch",
                             service_config=service_config, timeout=180)

    def start_daemon(self, home: str, service_config: str | None = None) -> dict:
        p = self.cli(home, "supervisor", "start",
                     service_config=service_config, check=True, timeout=60)
        self.daemon_homes.append(home)
        return json.loads(p.stdout)

    def stop_daemon(self, home: str, force: bool = False) -> dict:
        args = ["supervisor", "stop"]
        if force:
            args.append("--force")
        p = self.cli(home, *args, check=False, timeout=60)
        try:
            return json.loads(p.stdout)
        except ValueError:
            return {"ok": False, "stdout": p.stdout, "stderr": p.stderr}

    def kill_daemon(self, home: str):
        p = self.cli(home, "supervisor", "status", check=False)
        st = json.loads(p.stdout)["supervisor"]
        pid = st.get("pid")
        if pid:
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass

    # ---- 故障代理 -------------------------------------------------
    def start_proxy(self, name: str, *, config_path: str) -> tuple[str, str, str]:
        """返回 (listen_addr, control_path, stats_path)。"""
        # 找一个空闲端口
        import socket
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        control = os.path.join(self.base, f"{name}-control.json")
        stats = os.path.join(self.base, f"{name}-stats.json")
        with open(control, "w", encoding="utf-8") as f:
            json.dump({"generation": 0, "drop_active": False, "rules": []}, f)
        p = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "fault_proxy.py"),
             "--listen", f"127.0.0.1:{port}",
             "--target", f"{SERVICE.host}:{SERVICE.port}",
             "--control", control, "--stats", stats],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        self.proxies.append(p)
        time.sleep(0.4)
        return f"127.0.0.1:{port}", control, stats

    def proxy_control(self, control: str, *, gen: int | None = None,
                      drop_active: bool | None = None, rules=None):
        with open(control, "r", encoding="utf-8") as f:
            c = json.load(f)
        if gen is not None:
            c["generation"] = gen
        if drop_active is not None:
            c["drop_active"] = drop_active
        if rules is not None:
            c["rules"] = rules
        with open(control + ".tmp", "w", encoding="utf-8") as f:
            json.dump(c, f)
        os.replace(control + ".tmp", control)

    def service_config(self, name: str, listen: str, password: str | None = None) -> str:
        host, port = listen.split(":")
        cfg = {"hostname": host, "port": int(port),
               "password": password if password is not None else SERVICE.password}
        path = os.path.join(self.base, f"{name}-service.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f)
        os.chmod(path, 0o600)
        return path

    # ---- 场景记录 -------------------------------------------------
    def begin(self, name: str):
        self.scenario = {"name": name, "started_at": now_ms(), "checks": [],
                         "notes": []}
        print(f"\n=== {name} ===")

    def end(self, ok: bool, error: str | None = None):
        if self.scenario is not None:
            self.scenario["ok"] = ok
            self.scenario["ended_at"] = now_ms()
            self.scenario["duration_ms"] = self.scenario["ended_at"] - self.scenario["started_at"]
            if error:
                self.scenario["error"] = error
            self.evidence["scenarios"].append(self.scenario)
            print(f"=== {self.scenario['name']}: {'OK' if ok else 'FAIL'} "
                  f"({self.scenario['duration_ms']}ms) ===")
            self.scenario = None

    def cleanup(self):
        for home in self.daemon_homes:
            try:
                self.stop_daemon(home, force=True)
            except Exception:
                pass
        for p in self.supervisors:
            if p.poll() is None:
                try:
                    p.terminate()
                except OSError:
                    pass
        for p in self.proxies:
            if p.poll() is None:
                try:
                    p.terminate()
                except OSError:
                    pass
        # 只等待本轮直接创建的子进程，确认代理/前台循环已退出再清理 session。
        for p in self.supervisors + self.proxies:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=10)
        # 包括 CLI 超时后未返回 ID 的派单；只读取本次创建的隔离台账。
        for home in self.homes:
            try:
                for row in self.rows(home, "SELECT session_id FROM attempts WHERE session_id IS NOT NULL"):
                    if row["session_id"] not in self.created_sessions:
                        self.created_sessions.append(row["session_id"])
            except sqlite3.Error:
                pass
        for sid in self.created_sessions:
            try:
                self.real.interrupt(sid)
            except Exception:
                pass
        time.sleep(0.5)
        for sid in self.created_sessions:
            try:
                self.real.delete_session(sid)
            except Exception:
                pass
        remaining = []
        for sid in self.created_sessions:
            try:
                if self.real.get_session(sid) is not None:
                    remaining.append(sid)
            except Exception:
                remaining.append(sid)
        self.evidence["cleanup"] = {"created_sessions": len(self.created_sessions),
                                    "remaining_sessions": remaining,
                                    "verified": not remaining}


def scenario_s1(h: H):
    h.begin("s1_two_workers_wake_collect_verify")
    home = h.home("s1")
    site = os.path.join(h.base, "s1")
    os.makedirs(site, exist_ok=True)
    coord = h.session("S1 coordinator", site)["id"]
    t1 = h.task_create(home, "S1 one", site, coord, [os.path.join(site, "one.txt")])
    t2 = h.task_create(home, "S1 two", site + "-b", coord,
                       [os.path.join(site + "-b", "two.txt")])
    os.makedirs(site + "-b", exist_ok=True)
    a1 = h.dispatch(home, t1, site,
                    "只执行一条 shell 命令：echo ONE > one.txt，然后用一句话回复 DONE-ONE。"
                    "不要修改其他文件。")
    a2 = h.dispatch(home, t2, site + "-b",
                    "只执行一条 shell 命令：echo TWO > two.txt，然后用一句话回复 DONE-TWO。"
                    "不要修改其他文件。")
    s1 = h.wait_outcome(a1["session_id"])
    s2 = h.wait_outcome(a2["session_id"])
    term = max((s1.get("time") or {}).get("updated") or 0,
               (s2.get("time") or {}).get("updated") or 0)
    check("workers succeeded", s1.get("outcome") == "succeeded"
          and s2.get("outcome") == "succeeded", (s1.get("outcome"), s2.get("outcome")))
    check("no notification before supervisor", len(h.notifs(home)) == 0)
    h.supervisor_once(home)
    wake_at = now_ms()
    ns = h.notifs(home, "task_id IN (?,?)", (t1, t2))
    check("two result notifications", len(ns) == 2,
          [{"kind": n["kind"], "state": n["state"], "task": n["task_id"]} for n in ns])
    check("notifications sent", all(n["state"] in ("sent", "acked") for n in ns),
          [n["state"] for n in ns])
    check("attempts collected", all(
        h.attempt(home, h.task(home, t)["current_attempt_id"])["status"]
        in ("result_collected", "result_missing")
        for t in (t1, t2)))
    ums = h.user_message_ids(coord)
    wake = [m for m in h.messages(coord, limit=50, order="desc")
            if m.get("type") == "user"
            and all(n["id"] in json.dumps(m.get("text") or m.get("payload") or "")
                    for n in ns)]
    check("single coalesced wake", len(wake) == 1, f"user_msgs={len(ums)}")
    wake_latency = wake_at - term
    h.scenario["metrics"] = {"terminal_to_wake_ms": wake_latency,
                             "human_continue_count": 0,
                             "notifications": len(ns)}
    h.real.interrupt(coord)
    for t, a in ((t1, a1), (t2, a2)):
        h.cli_json(home, "collect", "--attempt", a["attempt_id"])
        out = h.cli_json(home, "verify", "--attempt", a["attempt_id"], "--pass",
                         "--evidence", f"e2e:{t}", "--notes", "s1")
        check(f"accepted {t}", out["task"]["status"] == "accepted")
    h.end(True)


def scenario_s2(h: H):
    h.begin("s2_missing_deliverable_rework")
    home = h.home("s2")
    site = os.path.join(h.base, "s2")
    os.makedirs(site, exist_ok=True)
    coord = h.session("S2 coordinator", site)["id"]
    missing = os.path.join(site, "missing.txt")
    t = h.task_create(home, "S2 missing", site, coord, [missing])
    a = h.dispatch(home, t, site,
                   "只回复 DONE-MISSING。不要运行 shell，不要创建任何文件。")
    h.wait_outcome(a["session_id"])
    h.supervisor_once(home)
    ns = h.notifs(home, "task_id=?", (t,))
    check("missing notification", len(ns) == 1 and
          missing in json.dumps(ns[0]["payload"]), ns[0]["payload"] if ns else None)
    check("task not accepted", h.task(home, t)["status"] != "accepted",
          h.task(home, t)["status"])
    p = h.cli(home, "verify", "--attempt", a["attempt_id"], "--pass",
              "--evidence", "should-fail", check=False)
    check("verify gate exit 7", p.returncode == 7, p.returncode)
    check("still not accepted", h.task(home, t)["status"] != "accepted")
    first_idle = (h.get_session(a["session_id"]) or {}).get("time", {}).get("idle") or 0
    r = h.cli_json(home, "rework", "--task", t, "--session", a["session_id"],
                   "--feedback", "请补交交付物：运行 shell 命令 `echo FIX > missing.txt`，"
                                 "然后回复 FIXED。")
    # 同一 session 的第二次回合：等新的 idle 时间，再让 Supervisor 对账
    h.wait(lambda: ((h.get_session(a["session_id"]) or {}).get("outcome") is not None
                    and ((h.get_session(a["session_id"]) or {}).get("time", {})
                         .get("idle") or 0) > first_idle),
           timeout=300, what="rework session turn")
    # 服务繁忙时单次对账可能超时；带重试地观察到新尝试终态
    h.wait(lambda: (h.supervisor_once(home) and
                    h.attempt(home, r["attempt_id"]).get("terminal_at")),
           timeout=180, interval=5, what="rework terminal")
    h.cli_json(home, "collect", "--attempt", r["attempt_id"])
    out = h.cli_json(home, "verify", "--attempt", r["attempt_id"], "--pass",
                     "--evidence", "missing.txt repaired", "--notes", "s2")
    check("rework accepted", out["task"]["status"] == "accepted")
    old = h.attempt(home, a["attempt_id"])
    check("old attempt not accepted", old["status"] != "accepted", old["status"])
    h.end(True)


def scenario_s3(h: H):
    h.begin("s3_real_permission_and_form")
    home = h.home("s3")
    site = os.path.join(h.base, "s3")
    os.makedirs(site, exist_ok=True)
    coord = h.session("S3 coordinator", site)["id"]
    worker = h.session("S3 permission worker", site,
                       permissions=[{"action": "*", "resource": "*", "effect": "ask"}])["id"]
    t = h.task_create(home, "S3 permission", site, coord)
    a = h.dispatch(home, t, site,
                   "请调用 shell 工具执行 `echo PERM > perm.txt`。只做这一件事。",
                   session=worker)
    h.wait(lambda: any(p.get("sessionID") == worker
                       for p in h.real.permissions(site)), timeout=40,
           what="real permission.asked")
    h.supervisor_once(home)
    att = h.attempt(home, a["attempt_id"])
    check("attempt blocked_permission", att["status"] == "blocked_permission", att["status"])
    ns = h.notifs(home, "task_id=?", (t,))
    check("permission notification", len(ns) == 1 and
          ns[0]["kind"] == "blocked_permission", [n["kind"] for n in ns])
    check("no auto approval", any(p.get("sessionID") == worker
                                  for p in h.real.permissions(site)))
    plist = h.cli_json(home, "permission", "list", "--directory", site,
                       "--session", worker)["permissions"]
    check("permission list", len(plist) >= 1, plist)
    perm = plist[0]
    h.cli_json(home, "permission", "reply", "--session", worker,
               "--request", perm["id"], "--decision", "reject",
               "--message", "e2e cleanup")
    h.wait(lambda: not any(p.get("sessionID") == worker
                           for p in h.real.permissions(site)), timeout=20,
           what="permission resolved")

    # 表单等待：独立 idle session 与独立目录，避免与权限现场冲突
    form_site = site + "-form"
    os.makedirs(form_site, exist_ok=True)
    form_worker = h.session("S3 form worker", form_site)["id"]
    t2 = h.task_create(home, "S3 form", form_site, coord)
    a2 = h.dispatch(home, t2, form_site, "", session=form_worker, no_prompt=True)
    # dispatch with empty prompt still sends footer；改为 no-prompt 更干净
    # 这里直接使用已创建的 attempt，创建一个真实 form
    form = h.real._post(f"/api/session/{form_worker}/form", {
        "title": "E2E form", "fields": [
            {"key": "answer", "type": "string", "title": "请回答"}
        ]
    })["data"]
    h.supervisor_once(home)
    att2 = h.attempt(home, a2["attempt_id"])
    check("attempt blocked_form", att2["status"] == "blocked_form", att2["status"])
    ns2 = h.notifs(home, "task_id=?", (t2,))
    check("form notification", len(ns2) == 1 and ns2[0]["kind"] == "blocked_form",
          [n["kind"] for n in ns2])
    # 清理
    h.real._request("DELETE", f"/api/session/{form_worker}/form/{form['id']}")
    h.cli_json(home, "task", "cancel", t, "--reason", "e2e")
    h.cli_json(home, "task", "cancel", t2, "--reason", "e2e")
    h.end(True)


def scenario_s4(h: H):
    h.begin("s4_long_tool_not_stalled")
    home = h.home("s4")
    site = os.path.join(h.base, "s4")
    os.makedirs(site, exist_ok=True)
    coord = h.session("S4 coordinator", site)["id"]
    t = h.task_create(home, "S4 long tool", site, coord)
    # 先启动 Supervisor，再派单，确保 tool.called 事件不会落在监听窗口之前
    h.start_daemon(home)
    a = h.dispatch(home, t, site,
                   "必须调用 shell 工具执行 `sleep 40`，等待命令返回后回复 LONG-DONE。",
                   stall_after=10, check=False)
    if not a.get("ok"):
        if a.get("exit") != 6:
            raise CheckFail(f"dispatch failed: {a}")
        detail = json.loads(a["stdout"])["detail"]
        a = {"attempt_id": detail["attempt_id"], "session_id": detail["session_id"]}
        # 响应超时/丢失：让 Supervisor 用同一 ID 对账，不重投新工作
        h.supervisor_once(home)
    time.sleep(18)
    att = h.attempt(home, a["attempt_id"])
    check("still running with active tool", att["status"] in ("running", "dispatched"),
          att["status"])
    check("active tool recorded", bool(att["metadata"].get("active_tool")),
          att["metadata"].get("active_tool"))
    stalls = h.notifs(home, "task_id=? AND kind='stall'", (t,))
    check("no false stall during tool", len(stalls) == 0, [n["kind"] for n in stalls])
    h.wait_outcome(a["session_id"], timeout=120)
    h.wait(lambda: h.task(home, t)["status"] in ("result_received", "accepted"),
           timeout=60, what="auto collect")
    h.stop_daemon(home)
    stalls = h.notifs(home, "task_id=? AND kind='stall'", (t,))
    check("no stall after completion", len(stalls) == 0)
    h.cli_json(home, "collect", "--attempt", a["attempt_id"])
    out = h.cli_json(home, "verify", "--attempt", a["attempt_id"], "--pass",
                     "--evidence", "LONG-DONE", "--notes", "s4")
    check("accepted", out["task"]["status"] == "accepted")
    h.end(True)


def scenario_s5(h: H):
    h.begin("s5_dispatch_response_lost")
    home = h.home("s5")
    site = os.path.join(h.base, "s5")
    os.makedirs(site, exist_ok=True)
    coord = h.session("S5 coordinator", site)["id"]
    listen, control, _ = h.start_proxy("s5", config_path="")
    cfg = h.service_config("s5", listen)
    h.proxy_control(control, gen=1, rules=[
        {"method": "POST", "path_re": r"/api/session/[^/]+/prompt",
         "drop_response": True}
    ])
    t = h.task_create(home, "S5 lost", site, coord, [os.path.join(site, "lost.txt")])
    p = h.cli(home, "dispatch", "--task", t, "--new-session", "--directory", site,
              "--agent", "build", "--model", MODEL,
              "--prompt", "只执行 shell 命令 `echo LOST > lost.txt`，然后回复 DONE-LOST。",
              service_config=cfg, check=False)
    check("dispatch exit 6", p.returncode == 6, f"{p.returncode} {p.stderr[:300]}")
    # 真实服务中应已看到 prompt（请求已受理）
    # 从错误 JSON 中取 session/attempt
    err = json.loads(p.stdout) if p.stdout.strip().startswith("{") else {}
    sess = err.get("detail", {}).get("session_id")
    att_id = err.get("detail", {}).get("attempt_id")
    check("attempt preserved", bool(att_id and sess), (att_id, sess))
    h.wait(lambda: any(m.get("id") for m in h.messages(sess, type_="user")),
           timeout=20, what="prompt admitted")
    check("still dispatch_unknown", h.attempt(home, att_id)["dispatch_state"] == "unknown")
    h.proxy_control(control, gen=2, rules=[])
    h.supervisor_once(home, service_config=cfg)
    h.wait_outcome(sess)
    h.supervisor_once(home, service_config=cfg)
    h.cli_json(home, "collect", "--attempt", att_id, service_config=cfg)
    out = h.cli_json(home, "verify", "--attempt", att_id, "--pass",
                     "--evidence", "lost response recovered", "--notes", "s5",
                     service_config=cfg)
    check("accepted after reconcile", out["task"]["status"] == "accepted")
    uid = [m["id"] for m in h.messages(sess, limit=100)
           if m.get("type") == "user"]
    check("one user prompt only", len(uid) == 1, uid)
    h.end(True)


def scenario_s6(h: H):
    h.begin("s6_disconnect_reconnect_reconcile")
    home = h.home("s6")
    with open(os.path.join(home, "config.json"), "w", encoding="utf-8") as f:
        json.dump({"reconcile_interval": 60, "heartbeat_interval": 1,
                   "ack_timeout": 90, "desktop_alerts": False}, f)
    site = os.path.join(h.base, "s6")
    os.makedirs(site, exist_ok=True)
    coord = h.session("S6 coordinator", site)["id"]
    listen, control, _ = h.start_proxy("s6", config_path="")
    cfg = h.service_config("s6", listen)
    h.start_daemon(home, service_config=cfg)
    h.wait(lambda: json.loads(h.cli(home, "supervisor", "status").stdout)
           ["supervisor"]["connected"], timeout=20, what="sse connected")
    t = h.task_create(home, "S6 disconnect", site, coord, [os.path.join(site, "d.txt")])
    a = h.dispatch(home, t, site,
                   "只执行 shell 命令 `echo D > d.txt`，然后回复 DONE-D。")
    # 断流并暂时阻止重连
    h.proxy_control(control, gen=1, drop_active=True, rules=[
        {"method": "GET", "path_re": r"/api/event", "status": 503}
    ])
    time.sleep(3)
    h.wait_outcome(a["session_id"], timeout=120)
    h.proxy_control(control, gen=2, drop_active=False, rules=[])
    h.wait(lambda: h.task(home, t)["status"] in ("result_received", "accepted"),
           timeout=60, what="reconnect reconcile")
    ns = h.notifs(home, "task_id=?", (t,))
    check("one result notification after reconnect", len(ns) == 1, len(ns))
    h.stop_daemon(home)
    h.cli_json(home, "collect", "--attempt", a["attempt_id"])
    out = h.cli_json(home, "verify", "--attempt", a["attempt_id"], "--pass",
                     "--evidence", "reconnect", "--notes", "s6")
    check("accepted", out["task"]["status"] == "accepted")
    h.end(True)


def scenario_s7(h: H):
    h.begin("s7_crash_restart_single_writer")
    home = h.home("s7")
    site = os.path.join(h.base, "s7")
    os.makedirs(site, exist_ok=True)
    coord = h.session("S7 coordinator", site)["id"]
    t = h.task_create(home, "S7 crash", site, coord, [os.path.join(site, "c.txt")])
    a = h.dispatch(home, t, site,
                   "只执行 shell 命令 `sleep 15; echo C > c.txt`，然后回复 DONE-C。")
    h.start_daemon(home)
    h.wait(lambda: h.attempt(home, a["attempt_id"])["status"] == "running",
           timeout=40, what="worker running")
    # 第二个 supervisor 必须被单写者拒绝
    p = h.cli(home, "supervisor", "run", "--once", check=False)
    check("second writer rejected", p.returncode == 5, p.returncode)
    h.kill_daemon(home)
    time.sleep(0.5)
    h.start_daemon(home)
    h.wait_outcome(a["session_id"], timeout=120)
    h.wait(lambda: h.task(home, t)["status"] in ("result_received", "accepted"),
           timeout=60, what="restart reconcile")
    ns = h.notifs(home, "task_id=?", (t,))
    check("notification preserved", len(ns) == 1, len(ns))
    h.stop_daemon(home)
    h.cli_json(home, "collect", "--attempt", a["attempt_id"])
    out = h.cli_json(home, "verify", "--attempt", a["attempt_id"], "--pass",
                     "--evidence", "restart", "--notes", "s7")
    check("accepted", out["task"]["status"] == "accepted")
    h.end(True)


def scenario_s8(h: H):
    h.begin("s8_endpoint_change")
    home = h.home("s8")
    with open(os.path.join(home, "config.json"), "w", encoding="utf-8") as f:
        json.dump({"reconcile_interval": 2, "heartbeat_interval": 1,
                   "ack_timeout": 90, "desktop_alerts": False}, f)
    site = os.path.join(h.base, "s8")
    os.makedirs(site, exist_ok=True)
    coord = h.session("S8 coordinator", site)["id"]
    listen_a, _, _ = h.start_proxy("s8a", config_path="")
    cfg = h.service_config("s8", listen_a)
    h.start_daemon(home, service_config=cfg)
    h.wait(lambda: json.loads(h.cli(home, "supervisor", "status").stdout)
           ["supervisor"]["connected"], timeout=20, what="sse connected")
    t = h.task_create(home, "S8 endpoint", site, coord, [os.path.join(site, "e.txt")])
    a = h.dispatch(home, t, site,
                   "只执行 shell 命令 `echo E > e.txt`，然后回复 DONE-E。")
    # 关闭旧代理，启动新端口代理，并改写 service config
    h.proxies[-1].terminate()
    listen_b, _, _ = h.start_proxy("s8b", config_path="")
    cfg_b = h.service_config("s8", listen_b)
    check("new endpoint differs", listen_a != listen_b)
    h.proxy_control  # noqa
    # 原 supervisor 通过环境变量读取旧 cfg 路径；重写同一路径
    with open(cfg, "r", encoding="utf-8") as f:
        data = json.load(f)
    data["port"] = int(listen_b.split(":")[1])
    with open(cfg, "w", encoding="utf-8") as f:
        json.dump(data, f)
    h.wait_outcome(a["session_id"], timeout=120)
    h.wait(lambda: h.task(home, t)["status"] in ("result_received", "accepted"),
           timeout=90, what="endpoint failover reconcile")
    hb = json.loads(h.cli(home, "supervisor", "status").stdout)["supervisor"]
    check("heartbeat follows new endpoint",
          str(hb.get("service", {}).get("port")) == listen_b.split(":")[1],
          hb.get("service"))
    h.stop_daemon(home)
    h.cli_json(home, "collect", "--attempt", a["attempt_id"])
    out = h.cli_json(home, "verify", "--attempt", a["attempt_id"], "--pass",
                     "--evidence", "endpoint change", "--notes", "s8")
    check("accepted", out["task"]["status"] == "accepted")
    h.end(True)


def scenario_s9(h: H):
    h.begin("s9_429_403_cancel")
    home = h.home("s9")
    site = os.path.join(h.base, "s9")
    os.makedirs(site, exist_ok=True)
    coord = h.session("S9 coordinator", site)["id"]
    listen, control, _ = h.start_proxy("s9", config_path="")
    cfg = h.service_config("s9", listen)
    t = h.task_create(home, "S9 backoff", site, coord, [os.path.join(site, "b.txt")])
    a = h.dispatch(home, t, site,
                   "只执行 shell 命令 `echo B > b.txt`，然后回复 DONE-B。")
    h.wait_outcome(a["session_id"])
    h.proxy_control(control, gen=1, rules=[
        {"method": "POST", "path_re": r"/api/session/[^/]+/prompt", "status": 429}
    ])
    h.supervisor_once(home, service_config=cfg)
    ns = h.notifs(home, "task_id=?", (t,))
    check("429 keeps notification pending", len(ns) == 1 and
          ns[0]["state"] in ("pending", "sent"), ns[0]["state"] if ns else None)
    check("429 attempts recorded", ns[0]["attempts"] >= 1, ns[0]["attempts"])
    h.proxy_control(control, gen=2, rules=[])
    time.sleep(3)
    h.supervisor_once(home, service_config=cfg)
    ns = h.notifs(home, "task_id=?", (t,))
    check("429 recovered after backoff", ns and ns[0]["state"] in ("sent", "acked"),
          ns[0]["state"] if ns else None)

    # 403 唤醒通道
    h.proxy_control(control, gen=3, rules=[
        {"method": "POST", "path_re": r"/api/session/[^/]+/prompt", "status": 403}
    ])
    h.cli_json(home, "notify", "retry", ns[0]["id"], service_config=cfg)
    h.supervisor_once(home, service_config=cfg)
    alerts = h.rows(home, "SELECT * FROM alerts WHERE kind='wake_auth'")
    check("403 auth alert", len(alerts) == 1, len(alerts))
    h.proxy_control(control, gen=4, rules=[])

    # 人工取消：不自动复活
    cancel_site = site + "-cancel"
    os.makedirs(cancel_site, exist_ok=True)
    t2 = h.task_create(home, "S9 cancel", cancel_site, coord)
    a2 = h.dispatch(home, t2, cancel_site,
                    "只执行 shell 命令 `sleep 20`，然后回复 DONE-CANCEL。")
    h.cli_json(home, "task", "cancel", t2, "--reason", "e2e-cancel", "--interrupt")
    h.wait_outcome(a2["session_id"], timeout=60)
    h.supervisor_once(home, service_config=cfg)
    task2 = h.task(home, t2)
    check("cancelled remains cancelled", task2["status"] == "cancelled", task2["status"])
    n_attempts = h.rows(home, "SELECT * FROM attempts WHERE task_id=?", (t2,))
    check("no resurrection attempt", len(n_attempts) == 1, len(n_attempts))
    h.end(True)


def scenario_s10(h: H):
    h.begin("s10_provider_failure_no_autoretry")
    home = h.home("s10")
    site = os.path.join(h.base, "s10")
    os.makedirs(site, exist_ok=True)
    coord = h.session("S10 coordinator", site)["id"]
    t = h.task_create(home, "S10 provider fail", site, coord)
    a = h.dispatch(home, t, site, "回复 FAILTEST 即可。",
                   model="opencode-go/definitely-not-a-model")
    h.wait_outcome(a["session_id"])
    h.supervisor_once(home)
    task = h.task(home, t)
    check("task blocked execution_failed", task["status"] == "blocked" and
          task["blocked_reason"] == "execution_failed", task)
    ns = h.notifs(home, "task_id=?", (t,))
    check("execution_failed notification", len(ns) == 1 and
          ns[0]["kind"] == "execution_failed", [n["kind"] for n in ns])
    atts = h.rows(home, "SELECT * FROM attempts WHERE task_id=?", (t,))
    check("no auto retry attempt", len(atts) == 1, len(atts))
    h.end(True)


def scenario_s11(h: H):
    h.begin("s11_duplicate_prompt_id")
    home = h.home("s11")
    site = os.path.join(h.base, "s11")
    os.makedirs(site, exist_ok=True)
    sid = h.session("S11 dup", site)["id"]
    msg = "msg_ocorche2edup" + uuid.uuid4().hex[:8]
    h.cli_json(home, "send", "--session", sid, "--text", "回复 DUP-OK。",
               "--message-id", msg)
    h.cli_json(home, "send", "--session", sid, "--text", "回复 DUP-OK。",
               "--message-id", msg)
    ids = h.user_message_ids(sid)
    check("one message for duplicate id", ids.count(msg) == 1, ids)
    h.end(True)


def scenario_s12(h: H):
    h.begin("s12_coordinator_failure_recovery")
    home = h.home("s12")
    with open(os.path.join(home, "config.json"), "w", encoding="utf-8") as f:
        json.dump({"reconcile_interval": 2, "heartbeat_interval": 1,
                   "recovery_delay": 1, "ack_timeout": 90,
                   "dispatch_unknown_grace": 1,
                   "desktop_alerts": False}, f)
    site = os.path.join(h.base, "s12")
    os.makedirs(site, exist_ok=True)
    bad = h.session("S12 bad coordinator", site,
                    model="opencode-go/definitely-not-a-model")["id"]
    h.cli_json(home, "coordinator", "claim", "--project", "cfail",
               "--session", bad)
    worker = h.session("S12 idle worker", site)["id"]
    t = h.task_create(home, "S12 task", site, bad, project="cfail")
    a = h.dispatch(home, t, site, "", session=worker, no_prompt=True,
                   coordinator=bad)
    # 给 idle 判定留出 grace，再执行一次对账
    time.sleep(2)
    # 第一次 once：把“idle 未确认”通知投递给坏协调者；坏模型立即失败
    h.supervisor_once(home)
    h.wait(lambda: any(r["target_session"] == bad and r["state"] == "sent"
                       for r in h.notifs(home, "task_id=?", (t,))),
           timeout=30, what="wake sent to bad coordinator")
    # 等坏模型的执行真正进入终态，再做一次对账
    h.wait(lambda: (h.get_session(bad) or {}).get("outcome") == "failed",
           timeout=30, what="bad coordinator execution failed")
    first_idle = (h.get_session(bad) or {}).get("time", {}).get("idle") or 0
    # 第二次 once：观察协调者失败并准备恢复
    h.supervisor_once(home)
    lease = h.cli_json(home, "coordinator", "status")["leases"][0]
    check("failure counted", lease["failure_count"] >= 1, lease["failure_count"])
    alerts = h.rows(home, "SELECT * FROM alerts WHERE kind='coordinator_failed'")
    check("coordinator failure alert", len(alerts) >= 1, len(alerts))
    rec = h.rows(home, "SELECT * FROM notifications WHERE kind='coordinator_recovery'")
    check("bounded recovery queued", len(rec) == 1, len(rec))
    # 恢复投递后再次失败 → recovery_blocked
    time.sleep(2)
    h.supervisor_once(home)
    h.wait(lambda: ((h.get_session(bad) or {}).get("time", {}).get("idle") or 0) > first_idle
           and (h.get_session(bad) or {}).get("outcome") == "failed",
           timeout=30, what="bad coordinator failed again after recovery")
    h.supervisor_once(home)
    lease = h.cli_json(home, "coordinator", "status")["leases"][0]
    check("recovery blocked after repeat", lease["recovery_blocked"] == 1, lease)
    s = h.get_session(bad)
    check("no model switch", s["model"]["id"] == "definitely-not-a-model", s["model"])
    h.cli_json(home, "task", "cancel", t, "--reason", "e2e")
    h.end(True)


SCENARIOS = {
    "s1": scenario_s1,
    "s2": scenario_s2,
    "s3": scenario_s3,
    "s4": scenario_s4,
    "s5": scenario_s5,
    "s6": scenario_s6,
    "s7": scenario_s7,
    "s8": scenario_s8,
    "s9": scenario_s9,
    "s10": scenario_s10,
    "s11": scenario_s11,
    "s12": scenario_s12,
}


def main():
    global SERVICE, MODEL
    ap = argparse.ArgumentParser()
    selection = ap.add_mutually_exclusive_group(required=True)
    selection.add_argument("--only", default="")
    selection.add_argument("--all", action="store_true")
    ap.add_argument("--root", default="/tmp/opencode/oc-orch-e2e")
    ap.add_argument("--model", default=MODEL)
    args = ap.parse_args()
    names = (list(SCENARIOS) if args.all
              else [x.strip() for x in args.only.split(",") if x.strip()])
    if not names or any(name not in SCENARIOS for name in names):
        ap.error("请选择至少一个有效场景")
    SERVICE, MODEL = load_service(), args.model
    h = H(args.root)
    ok_all = True
    try:
        for name in names:
            fn = SCENARIOS[name]
            try:
                fn(h)
            except Exception as e:
                ok_all = False
                h.end(False, f"{type(e).__name__}: {e}")
                traceback.print_exc()
                try:
                    h.cleanup()
                except Exception:
                    pass
        h.evidence["ended_at"] = now_ms()
        h.cleanup()
        ok_all = ok_all and h.evidence["cleanup"]["verified"]
        h.evidence["ok"] = ok_all
        out = os.path.join(h.base, "evidence.json")
        with open(out, "w", encoding="utf-8") as f:
            json.dump(h.evidence, f, ensure_ascii=False, indent=1)
        print(f"\nEVIDENCE {out}")
        print("ALL PASS" if ok_all else "FAILURES PRESENT")
        return 0 if ok_all else 1
    finally:
        h.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())

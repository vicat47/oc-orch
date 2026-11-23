#!/usr/bin/env python3
"""显式受控 S4 真服务验证；独立临时 home，仅创建/清理本脚本 session。

使用 --run 才读取服务凭据和启动模型，不纳入 test_*.py 离线发现。
验证输入已执行后的责任接续，不把消息送达/模型回合成功当业务验收。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="显式执行专用真实 session 验收")
    parser.add_argument("--model", metavar="PROVIDER/MODEL", help="专用测试模型")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--evidence", type=Path,
                        default=ROOT / "evidence/2026-11-11-s4-responsibility/real-api.json")
    args = parser.parse_args(argv)
    if not args.run:
        parser.print_help()
        return 0

    from oc_orch.client import Client
    from oc_orch.composition import application, make_managed_inputs, make_responsibilities
    from oc_orch.config import DEFAULTS
    from oc_orch.supervisor import Supervisor
    from oc_orch.util import now_ms

    provider, _, model_id = (args.model or "").partition("/")
    if not provider or not model_id:
        parser.error("--run 需要 --model PROVIDER/MODEL")
    model = {"providerID": provider, "id": model_id}
    client = Client(timeout=30)
    root = Path(tempfile.mkdtemp(prefix="s4-real-", dir="/tmp/opencode"))
    home, site = root / "ledger", root / "site"
    home.mkdir(mode=0o700)
    site.mkdir(mode=0o700)
    cfg = {**DEFAULTS, "auto_collect": False, "desktop_alerts": False,
           "ack_timeout": 1200, "stall_after": 1800}
    evidence = {"schema": "oc-orch.s4-real-continuity.v1",
                "started_at": now_ms(), "home": str(home),
                "site": str(site), "model": model, "checks": [],
                "sessions": [], "cleanup": [], "status": "running",
                "limitations": ["只覆盖专用会话输入接续，不代表生产已部署或业务项目已恢复",
                                 "worker 纯文本回报按业务未知核收；结构化协议另有离线断言"],
                "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                   for p in sorted([*(ROOT / "src").rglob("*.py"),
                                                    Path(__file__).resolve(),
                                                    ROOT / "scripts/managed_input_send.py"])}}
    args.evidence.parent.mkdir(parents=True, exist_ok=True)
    def save():
        args.evidence.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + chr(10))
    def check(name, condition, detail=None):
        if not condition:
            raise AssertionError(f"{name}: {detail}")
        evidence["checks"].append({"name": name, "passed": True, "detail": detail})
        print("PASS", name, flush=True)
        save()
    def wait(fn, what):
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            result = fn()
            if result:
                return result
            time.sleep(2)
        raise TimeoutError(what)
    sup = None
    try:
        coord = "ses_s4e2e" + uuid.uuid4().hex[:16]
        worker = "ses_s4e2e" + uuid.uuid4().hex[:16]
        evidence["sessions"] = [coord, worker]
        save()
        for sid, title in [(coord, "S4 专用接续核收"), (worker, "S4 专用执行worker")]:
            client.create_session(session_id=sid, directory=str(site), title=title,
                                  agent="general", model=model,
                                  metadata={"s4_continuity_e2e": True},
                                  permissions=[{"action": "*", "resource": "*", "effect": "deny"}])
            actual = client.get_session(sid)
            actual_model = (actual or {}).get("model") or {}
            check("专用session模型", all(actual_model.get(k) == v for k, v in model.items()),
                  {"session": sid, "model": actual_model})

        sup = Supervisor(str(home), cfg=cfg, client=client)
        task = sup.ledger.create_task(
            project="s4-dedicated-integration", title="执行后回报须交回协调者",
            objective="专用worker完成一次无工具文本回报，再执行只读核收并回报缺材料",
            acceptance=["真实输入所属执行可核验", "旧ACK不吞新核收待办"],
            coordinator_session=coord, write_scope=[str(site)])
        app = application(sup.ledger, lambda: client, cfg)
        dispatched = app.dispatch(
            task["id"], directory=str(site), coordinator=coord, session=worker,
            model=f"{model['providerID']}/{model['id']}",
            prompt="这是机制集成测试，不使用任何工具、不读写文件、不委派。仅回复 S4-INITIAL-READY 并结束。")
        aid = dispatched["attempt_id"]
        evidence.update(task_id=task["id"], attempt_id=aid, initial_message_id=dispatched["message_id"])
        def collected_terminal():
            sup.cycle(dispatch=False)
            att = sup.ledger.get_attempt(aid)
            return att if att["status"] == "terminal_succeeded" else None
        sealed = wait(collected_terminal, "initial real execution terminal")
        check("原输入真实执行封存", bool(sealed.get("terminal_at")), sealed["status"])
        result = app.collect(aid)
        check("真实结果collect", result.get("status") == "result_collected")
        collected = sup.ledger.get_attempt(aid)
        collected_artifact = json.loads(Path(collected["result_path"]).read_text())
        evidence["initial_result"] = {"result_path": collected["result_path"],
                                      "result_version": result.get("result_version"),
                                      "classification": result.get("classification"),
                                      "text": collected_artifact.get("text")}
        check("初始文本交付证据", "S4-INITIAL-READY" in json.dumps(collected_artifact))
        old = sup.ledger.list_notifications(target_session=coord, limit=100)
        check("旧结果通知存在", bool(old))
        sup.ledger.ack_notifications([n["id"] for n in old], actor="e2e-harness:coordinator")
        evidence["old_notification_ids"] = [n["id"] for n in old]
        sealed = sup.ledger.get_attempt(aid)
        managed = make_managed_inputs(sup.conn, str(home), client=lambda: client)
        followup_mid = "msg_s4e2e" + uuid.uuid4().hex[:18]
        command = [sys.executable, "-B", str(ROOT / "scripts/managed_input_send.py"),
                   "--apply", "--home", str(home), "--session", worker,
                   "--message-id", followup_mid, "--owner", "dedicated-coordinator",
                   "--task", task["id"], "--continuation-target", coord,
                   "--deadline", str(now_ms() + 600000), "--work-kind", "read_only",
                   "--material", sealed["metadata"]["execution"]["baseline"]["path"],
                   "--text", "机制集成测试，只读核收。不要使用工具，不读写任何文件。仅回复：S4-NEEDS-MATERIAL 当前缺少基线材料，请协调者提供。随后结束。"]
        process = subprocess.run(command, cwd=ROOT, text=True, capture_output=True,
                                 env={**os.environ, "OC_ORCH_HOME": str(home)}, timeout=60)
        evidence["external_sender"] = {"path": "scripts/managed_input_send.py",
                                       "exit_code": process.returncode,
                                       "stdout": process.stdout[-6000:],
                                       "stderr": process.stderr[-3000:]}
        check("外部发送脚本真实接入", process.returncode == 0, process.stderr[-300:])
        matching = [i for i in managed.list_inputs(session_id=worker)["inputs"]
                    if i.get("message_id") == followup_mid]
        check("脚本原输入身份持久", len(matching) == 1)
        input_view = managed.get(matching[0]["input_id"])["input"]
        sent = {"input": input_view, "responsibility":
                make_responsibilities(sup.ledger).get_responsibility(
                    input_view["responsibility_id"])}
        input_id = sent["input"]["input_id"]
        evidence["responsibility_id"] = sent["responsibility"]["id"]
        evidence["followup_input_id"] = input_id
        evidence["followup_message_id"] = sent["input"]["message_id"]
        check("发送前责任已落库", bool(sent.get("responsibility")))
        check("绑定真实结果版本", sent["responsibility"]["result_version"] ==
              sealed["metadata"]["result"]["version"])
        # 新实例仅 REST 补证，证明接续不依赖本进程先收到 SSE。
        sup.conn.close()
        sup = Supervisor(str(home), cfg=cfg, client=client)
        def continuation_ready():
            sup.cycle(dispatch=False)
            rows = [n for n in sup.ledger.list_notifications(target_session=coord, limit=100)
                    if n["kind"] == "continuation_ready"]
            return rows[0] if rows else None
        continuation = wait(continuation_ready, "REST continuation after restart")
        check("ACK后新接续待办", continuation["state"] == "pending", continuation["id"])
        check("无结构化回报转协调判断",
              continuation["payload"].get("classification") == "AWAITING_REVIEW"
              and continuation["payload"].get("auto_accept") is False)
        check("旧通知ACK未被覆盖", all(sup.ledger.get_notification(n["id"])["state"]
              == "acked" for n in old))
        after = sup.ledger.get_attempt(aid)
        check("旧执行与结果保持封存", all(after[k] == sealed[k] for k in
              ["terminal_at", "result_path", "site_released_at"]))
        check("仍待业务验收", sup.ledger.get_task(task["id"])["status"] == "result_received"
              and after["verification"] is None)
        sup.dispatch_notifications()
        notif = sup.ledger.get_notification(continuation["id"])
        evidence["continuation_notification_id"] = notif["id"]
        evidence["coordinator_message_id"] = notif.get("message_id")
        check("接续走标准通知受理", bool(notif.get("message_id")))
        msg = wait(lambda: client.get_message(coord, notif["message_id"]),
                   "dedicated coordinator received notification")
        check("专用协调者收到原消息", msg.get("id") == notif["message_id"] and msg.get("type") == "user")
        sup.ledger.ack_notifications([notif["id"]], actor="e2e-harness:coordinator")
        receipt = make_managed_inputs(sup.conn, str(home), client=lambda: client).get(input_id)
        evidence["followup_receipt"] = receipt
        check("输入执行证据已写回", receipt["input"].get("execution") == "succeeded",
              receipt["input"].get("execution"))
        check("通知ACK不替代业务确认", receipt["input"].get("business_ack") != "acknowledged")
        current_managed = make_managed_inputs(sup.conn, str(home), client=lambda: client)
        acked = current_managed.acknowledge(
            input_id, value=True, owner="dedicated-coordinator", confirm=input_id,
            expected_revision=receipt["input"]["revision"],
            reason="专用测试协调者核收了缺材料回报；与原任务 verify 分开")
        check("显式业务确认独立记账", acked["input"]["business_ack"] == "acknowledged"
              and sup.ledger.get_task(task["id"])["status"] == "result_received")
        verified = application(sup.ledger, lambda: client, cfg)
        verification = verified.verify(aid, passed=True)
        check("正常verify闭合专用任务", verification.get("verification", {}).get("classification") == "normal_effective",
              verification.get("verification", {}).get("classification"))
        sup.cycle(dispatch=False)
        check("accepted后推进责任关闭", not make_responsibilities(sup.ledger).list_open_responsibilities(task_id=task["id"]))
        snapshot_dir = args.evidence.parent / (args.evidence.stem + "-artifacts")
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "ledger-export.json").write_text(
            json.dumps(sup.ledger.export(), ensure_ascii=False, indent=2) + chr(10))
        result_files = []
        for source in sorted((home / "results").rglob("*")):
            if not source.is_file():
                continue
            relative = source.relative_to(home)
            target = snapshot_dir / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            result_files.append({"path": str(target.relative_to(args.evidence.parent)),
                                 "sha256": hashlib.sha256(target.read_bytes()).hexdigest()})
        evidence["artifacts"] = {"export": str((snapshot_dir / "ledger-export.json").relative_to(args.evidence.parent)),
                                 "result_files": result_files}
        evidence["status"] = "passed"
    except Exception as exc:
        evidence["status"] = "failed"
        evidence["error"] = {"type": type(exc).__name__, "message": str(exc)[:1000]}
        raise
    finally:
        if sup is not None:
            sup.conn.close()
        for sid in evidence["sessions"]:
            try:
                if sid in client.active_sessions():
                    client.interrupt(sid, resume=False)
                client.delete_session(sid)
                absent = client.get_session(sid) is None
                evidence["cleanup"].append({"session": sid, "absent": absent})
            except Exception as exc:
                evidence["cleanup"].append({"session": sid, "error": type(exc).__name__})
        evidence["finished_at"] = now_ms()
        if evidence["status"] == "passed" and not all(
                c.get("absent") is True for c in evidence["cleanup"]):
            evidence["status"] = "failed-cleanup"
        save()
        print("EVIDENCE", args.evidence, flush=True)
    return 0 if evidence["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())

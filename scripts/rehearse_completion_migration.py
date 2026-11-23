#!/usr/bin/env python3
"""M3 演练：旧数据离线兼容 / SQLite 一致备份 / 版本混用与回滚边界。

只做数据与迁移演练，不改业务代码：
- 固定点 9ffe247 旧代码经 `git archive` 只读提取到临时目录，不切换工作区；
- 全部在 /tmp/opencode 临时目录、真实 SQLite WAL 与 backup API 上执行；
- 必须存在"未 checkpoint 写入"以证明不能直接 copy 活动台账主文件；
- 备份结果目录被引用文件并 restore 验证引用一致；旧 ACK/accepted/历史坏 turn 不回写；
- 用旧代码独立进程只读新 schema1/v1 扩展，记录"可读"与"不可保持"的新业务约束；
- 演示版本混用必须被运行期拒绝或需新版本修复，绝不声称 schema1 不变即可无损回滚。

运行（在仓库根目录）：
    python3 scripts/rehearse_completion_migration.py
输出证据默认写入 evidence/2026-11-08-completion-integrity-remediation/migration/。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BASE = "9ffe2477ebd030d50d00f1a3e94ee3bdc875674f"
DEFAULT_EVIDENCE = (
    REPO / "evidence" / "2026-11-08-completion-integrity-remediation" / "migration"
)

# --------------------------------------------------------------------------
# 内嵌驱动：每个驱动是独立脚本，仅依赖 Python 标准库 + PYTHONPATH 指向的 oc_orch
# （旧代码 = git archive 提取的 src；新代码 = 仓库 src/）。驱动以
# __REHEARSE_JSON_BEGIN__ / __REHEARSE_JSON_END__ 打印 JSON 信封。
# --------------------------------------------------------------------------

_DRIVER_COMMON = r"""
import hashlib, json, os, shutil, sqlite3, sys

HOME = sys.argv[1]
LABEL = sys.argv[2] if len(sys.argv) > 2 else "driver"
E = {}


def sha256_file(p):
    with open(p, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def finish():
    print("__REHEARSE_JSON_BEGIN__")
    print(json.dumps(E, ensure_ascii=False, sort_keys=True))
    print("__REHEARSE_JSON_END__")
"""

_DRIVER_FAKE = r"""
# 最小 OpenCode 客户端替身（内嵌驱动不能使用三引号，故用注释说明）：
# - 旧代码收集路径：messages()（一次倒序分页）；
# - 新代码 F3 收集路径：message_page()（锚点/起界覆盖必须 self-consistent）；
# - F2 legacy 恢复补证：get_session()/get_message()/active_sessions()/inbox()。
# 协议要求 page 返回 {"data": [...], "cursor": {"next": null}}，
# 且锚点消息必须出现在 data 中（否则新 collect 判 anchor not found → 覆盖不足）。
class FakeClient:
    def __init__(self, messages, session=None, inbox_items=None):
        self._messages = list(messages)
        self._session = session
        self._inbox = list(inbox_items or [])
        self._by_id = {m["id"]: m for m in self._messages if m.get("id")}

    def _sorted(self, order="asc"):
        ms = sorted(self._messages, key=lambda m: (
            (m.get("time") or {}).get("created", 0), m["id"]))
        return list(reversed(ms)) if order == "desc" else ms

    def messages(self, session_id, limit=100, order="desc"):  # 旧代码收集
        return self._sorted(order)[:limit]

    def message_page(self, session_id, limit=100, order="desc", cursor=None):  # 新代码 F3
        if cursor is not None:
            return {"data": [], "cursor": {"next": None}}
        return {"data": self._sorted(order)[:limit], "cursor": {"next": None}}

    def get_message(self, session_id, message_id):  # read_anchor（锚点读取）
        return self._by_id.get(message_id)

    def get_session(self, session_id):  # F2 legacy 恢复 REST 事实
        return self._session

    def active_sessions(self):
        return {}

    def inbox(self, session_id):
        return list(self._inbox)
"""

_DRIVER_CFG = 'CFG = {"result_max_messages": 100, "result_store_limit": 200000, "collect_retry_interval": 15, "collect_max_attempts": 5}\n'

_TABLES = 'TABLES = ("meta", "tasks", "attempts", "turns", "deliverables", "events", "notifications", "alerts", "coordinator_leases", "audit")\n'


def _dump_fn():
    return (
        "def dump_rows(conn):\n"
        "    out = {}\n"
        "    for t in TABLES:\n"
        "        out[t] = [dict(r) for r in conn.execute('SELECT * FROM ' + t).fetchall()]\n"
        "    return out\n"
    )


# ---- 驱动 1：旧代码构建 legacy schema1 台账 + WAL 直拷 vs backup API ----
DRIVER_BUILD_LEGACY = _DRIVER_COMMON + r"""
E["sqlite3_version"] = sqlite3.sqlite_version
E["oc_orch_kind"] = "old"
E["script_label"] = LABEL

from oc_orch.adapters.sqlite_db import connect, init, transaction, meta_get
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters import results as results_mod

""" + _DRIVER_FAKE + _DRIVER_CFG + r"""
def walk_results(base=os.path.join(HOME, "results")):
    out = {}
    if not os.path.isdir(base):
        return out
    for root, _dirs, files in os.walk(base):
        for name in sorted(files):
            p = os.path.join(root, name)
            out[os.path.relpath(p, HOME)] = sha256_file(p)
    return out

conn = init(HOME)
ledger = Ledger(conn, HOME)
E["schema_version"] = meta_get(conn, "schema_version")
E["journal_mode"] = conn.execute("PRAGMA journal_mode").fetchone()[0]
ts = 1_700_000_000_000

def mk_work(name):
    d = os.path.join(HOME, name)
    os.makedirs(os.path.join(d, "out"), exist_ok=True)
    with open(os.path.join(d, "out", "report.md"), "w", encoding="utf-8") as f:
        f.write("legacy report v1")
    with open(os.path.join(d, "log.txt"), "w", encoding="utf-8") as f:
        f.write("legacy log v1")
    return d

# ---- L1：已验收（accepted）+ 已 ACK 通知 + 旧结果文件 ----
ledger.create_task(project="demo", title="legacy accepted (L1)",
                   coordinator_session="ses-c1", task_id="T-c1")
d1 = mk_work("work-a1")
ledger.create_attempt("T-c1", attempt_id="A-c1", directory=d1,
                      session_id="S-legacy-1", model="m", generation=1,
                      status="creating", deliverables=["out/report.md", "log.txt"])
ledger.record_dispatch("A-c1", "confirmed", started=True, task_status="running")
ledger.open_turn("A-c1", "ev-s1", ts + 1000)
ledger.transition_attempt("A-c1", "terminal_succeeded", actor="system", reason="old-terminal-L1")
ledger.close_turn("A-c1", "ev-e1", ts + 3000, "succeeded")
col1 = results_mod.collect_attempt(
    ledger, FakeClient([{"id": "msg-l1-1", "type": "assistant",
                         "time": {"created": ts + 1500},
                         "content": [{"type": "text", "text": "L1 已完成的实质结果文本"}]}]),
    "A-c1", CFG, auto=False)
n1 = ledger.enqueue_notification(kind="result_ready", target_session="ses-c1",
                                 dedupe_key="legacy-result:A-c1",
                                 payload={"summary": "L1 result ready", "task_id": "T-c1",
                                          "attempt_id": "A-c1"},
                                 project="demo", task_id="T-c1", attempt_id="A-c1", priority=6)
ledger.ack_notifications([n1["id"]], actor="coordinator")
results_mod.prepare_verification(ledger, "A-c1", result="pass", reason="legacy-accept-L1")
res1 = results_mod.verify_attempt(ledger, "A-c1", result="pass", reason="legacy-accept-L1")
att1 = ledger.get_attempt("A-c1")
task1 = ledger.get_task("T-c1")
assert att1["status"] == "accepted", att1["status"]
assert task1["status"] == "accepted", task1["status"]
n1row = ledger.get_notification(n1["id"])
assert n1row["state"] == "acked", n1row["state"]
E["L1"] = {"attempt_status": att1["status"], "task_status": task1["status"],
           "result_path": att1["result_path"],
           "result_file_sha256": sha256_file(att1["result_path"]),
           "result_collected_at": att1["result_collected_at"],
           "accepted_at": task1["accepted_at"],
           "verification_schema": (att1["verification"] or {}).get("schema"),
           "notification": {"id": n1row["id"], "state": n1row["state"],
                            "acked_at": n1row["acked_at"],
                            "dedupe_key": n1row["dedupe_key"]}}

# ---- L2：历史坏 turn——空结果已 ACK，真实完成事件随后才到（T29 形态） ----
ledger.create_task(project="demo", title="legacy bad empty result (L2)",
                   coordinator_session="ses-c2", task_id="T-c2")
d2 = mk_work("work-a2")
ledger.create_attempt("T-c2", attempt_id="A-c2", directory=d2,
                      session_id="S-legacy-2", model="m", generation=1,
                      status="creating", dispatch_msg_id="msg-n2-in",
                      deliverables=["out/report.md", "log.txt"])
ledger.record_dispatch("A-c2", "confirmed", started=True, task_status="running")
ledger.open_turn("A-c2", "ev-s2", ts + 5000)
ledger.transition_attempt("A-c2", "terminal_succeeded", actor="system", reason="old-terminal-L2-early")
ledger.close_turn("A-c2", "ev-e2b", ts + 6000, "succeeded")
col2 = results_mod.collect_attempt(ledger, FakeClient([]), "A-c2", CFG, auto=True)  # 空结果
n2 = ledger.enqueue_notification(kind="result_ready", target_session="ses-c2",
                                 dedupe_key="legacy-result:A-c2",
                                 payload={"summary": "L2 empty result (bad)", "task_id": "T-c2",
                                          "attempt_id": "A-c2"},
                                 project="demo", task_id="T-c2", attempt_id="A-c2", priority=6)
ledger.ack_notifications([n2["id"]], actor="coordinator")
ledger.record_event(event_id="ev-real-end", type_="session.execution.terminal",
                    session_id="S-legacy-2", task_id="T-c2", attempt_id="A-c2",
                    seq=7, version=None, created_ms=ts + 9000,
                    payload={"outcome": "succeeded", "ended_at": ts + 9000})
att2 = ledger.get_attempt("A-c2")
n2row = ledger.get_notification(n2["id"])
with open(att2["result_path"], "r", encoding="utf-8") as f:
    l2_data = json.load(f)
E["L2"] = {"attempt_status": att2["status"], "result_path": att2["result_path"],
           "result_text_len": len(l2_data.get("text") or ""),
           "terminal_at": att2["terminal_at"], "terminal_kind": att2["terminal_kind"],
           "turn_rows": ledger.turns("A-c2"),
           "notification": {"id": n2row["id"], "state": n2row["state"],
                            "acked_at": n2row["acked_at"], "dedupe_key": n2row["dedupe_key"]},
           "late_event": {"event_id": "ev-real-end", "created_ms": ts + 9000},
           "metadata_has_execution": bool((att2["metadata"] or {}).get("execution"))}
assert E["L2"]["result_text_len"] == 0, E["L2"]["result_text_len"]
assert E["L2"]["notification"]["state"] == "acked"
assert E["L2"]["metadata_has_execution"] is False

# ---- 未 checkpoint 写入：WAL 直拷 vs backup API ----
conn_w = connect(HOME)
conn_w.execute("PRAGMA wal_autocheckpoint=0")
marker = "N-wal-marker-1"
with transaction(conn_w):
    conn_w.execute(
        "INSERT INTO notifications(id, project, kind, target_session, task_id, attempt_id, "
        "dedupe_key, payload, state, priority, next_attempt_at, created_at, max_attempts, metadata) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (marker, "demo", "marker", None, None, None, "wal-marker", "{}",
         "pending", 0, ts + 999, ts + 999, 3, "{}"))
E["marker_nid"] = marker
wal_p = os.path.join(HOME, "ledger.db-wal")
E["wal_exists"] = os.path.exists(wal_p)
E["wal_bytes"] = os.path.getsize(wal_p) if E["wal_exists"] else 0
db_main = os.path.join(HOME, "ledger.db")
raw = os.path.join(HOME, "raw-copy.db")
shutil.copy2(db_main, raw)
E["raw_copy_sha256"] = sha256_file(raw)
try:
    c_raw = sqlite3.connect(raw)
    c_raw.row_factory = sqlite3.Row
    E["raw_copy_marker_count"] = c_raw.execute(
        "SELECT COUNT(*) AS c FROM notifications WHERE id=?", (marker,)).fetchone()["c"]
    E["raw_copy_attempt_count"] = c_raw.execute("SELECT COUNT(*) AS c FROM attempts").fetchone()["c"]
    E["raw_copy_integrity"] = c_raw.execute("PRAGMA integrity_check").fetchone()[0]
    E["raw_copy_ok"] = True
    c_raw.close()
except sqlite3.DatabaseError as exc:
    E["raw_copy_ok"] = False
    E["raw_copy_error"] = str(exc)[:200]
conn_r = connect(HOME)
bpath = os.path.join(HOME, "backup-consistent.db")
bdst = sqlite3.connect(bpath)
conn_r.backup(bdst)
bdst.close()
conn_r.close()
c_b = sqlite3.connect(bpath)
c_b.row_factory = sqlite3.Row
E["backup_marker_count"] = c_b.execute(
    "SELECT COUNT(*) AS c FROM notifications WHERE id=?", (marker,)).fetchone()["c"]
E["backup_attempt_count"] = c_b.execute("SELECT COUNT(*) AS c FROM attempts").fetchone()["c"]
E["backup_integrity"] = c_b.execute("PRAGMA integrity_check").fetchone()[0]
E["backup_journal_mode"] = c_b.execute("PRAGMA journal_mode").fetchone()[0]
E["backup_sha256"] = sha256_file(bpath)
c_b.close()

# ---- 汇总快照（供备份 manifest 与 restore 验证） ----
E["tables"] = {t: conn.execute("SELECT COUNT(*) AS c FROM " + t).fetchone()["c"]
               for t in ("meta", "tasks", "attempts", "turns", "deliverables",
                         "events", "notifications", "alerts", "audit")}
E["refs"] = []
E["attempts"] = {}
for aid in ("A-c1", "A-c2"):
    a = ledger.get_attempt(aid)
    E["refs"].append({"attempt": aid, "rel_path": os.path.relpath(a["result_path"], HOME)})
    E["attempts"][aid] = {"status": a["status"],
                          "rel_path": os.path.relpath(a["result_path"], HOME),
                          "abs_path": a["result_path"],
                          "result_collected_at": a["result_collected_at"]}
E["result_files"] = walk_results()
E["notification_rows"] = {n["id"]: {"state": n["state"], "acked_at": n["acked_at"],
                                    "dedupe_key": n["dedupe_key"]}
                          for n in ledger.list_notifications(limit=200)}
E["turn_rows"] = {aid: ledger.turns(aid) for aid in ("A-c1", "A-c2")}
E["event_rows"] = [dict(r) for r in conn.execute(
    "SELECT event_id,type,attempt_id,created_ms,duplicate FROM events ORDER BY event_id")]
assert E["backup_marker_count"] == 1, E["backup_marker_count"]
assert E["backup_attempt_count"] == 2, E["backup_attempt_count"]
assert E["backup_integrity"] == "ok"
if E["raw_copy_ok"]:
    assert E["raw_copy_marker_count"] == 0, E["raw_copy_marker_count"]
else:
    assert E.get("raw_copy_error"), "raw copy 失败必须有原因"
assert len(E["result_files"]) >= 2  # 两个旧 flat 结果文件

# 驱动内断言：真实数据与哈希，不打印空话
assert E["schema_version"] == "1"
assert E["journal_mode"] in ("wal", "WAL")
assert E["L1"]["attempt_status"] == "accepted" and E["L1"]["task_status"] == "accepted"
assert E["L1"]["notification"]["state"] == "acked"
assert E["L2"]["terminal_kind"] == "succeeded"
conn_w.close()
conn.close()
finish()
"""

# ---- 驱动 2：restore 后（旧代码）校验一致性 ----
DRIVER_VERIFY_RESTORED = _DRIVER_COMMON + r"""
E["sqlite3_version"] = sqlite3.sqlite_version
E["oc_orch_kind"] = "old"
E["script_label"] = LABEL
MANIFEST = sys.argv[3] if len(sys.argv) > 3 else None
assert MANIFEST, "need manifest path"

from oc_orch.adapters.sqlite_db import connect, meta_get
from oc_orch.adapters.sqlite_ledger import Ledger

m = json.load(open(MANIFEST, "r", encoding="utf-8"))
conn = connect(HOME)
conn.row_factory = sqlite3.Row
ledger = Ledger(conn, HOME)
E["schema_version"] = meta_get(conn, "schema_version")
E["journal_mode"] = conn.execute("PRAGMA journal_mode").fetchone()[0]
E["integrity"] = conn.execute("PRAGMA integrity_check").fetchone()[0]
E["tables_actual"] = {t: conn.execute("SELECT COUNT(*) AS c FROM " + t).fetchone()["c"]
                      for t in m["tables"]}
E["tables_match"] = all(E["tables_actual"][t] == m["tables"][t] for t in m["tables"])
E["refs_checked"] = []
E["refs_ok"] = True
for ref in m["refs"]:
    p = os.path.join(HOME, ref["rel_path"])
    exists = os.path.isfile(p)
    sha = sha256_file(p) if exists else None
    ok = bool(exists) and sha == m["files"].get(ref["rel_path"])
    E["refs_checked"].append({"attempt": ref["attempt"], "rel_path": ref["rel_path"],
                              "exists": bool(exists), "sha_matches": bool(ok)})
    E["refs_ok"] = E["refs_ok"] and bool(ok)
E["attempts_match"] = all(
    ledger.get_attempt(aid)["status"] == exp["status"]
    and ledger.get_attempt(aid)["result_collected_at"] == exp["result_collected_at"]
    and ledger.get_attempt(aid)["result_path"] == exp["abs_path"]
    for aid, exp in (m.get("attempts") or {}).items())
E["notification_rows_match"] = all(
    ledger.get_notification(nid)["state"] == exp["state"]
    and ledger.get_notification(nid)["acked_at"] == exp["acked_at"]
    and ledger.get_notification(nid)["dedupe_key"] == exp["dedupe_key"]
    for nid, exp in (m.get("notification_rows") or {}).items())
E["turn_rows_match"] = all(
    json.dumps(ledger.turns(aid), sort_keys=True) == json.dumps(rows, sort_keys=True)
    for aid, rows in (m.get("turn_rows") or {}).items())
cur_events = [dict(r) for r in conn.execute(
    "SELECT event_id,type,attempt_id,created_ms,duplicate FROM events ORDER BY event_id")]
E["events_match"] = json.dumps(cur_events, sort_keys=True) == json.dumps(
    m.get("event_rows") or [], sort_keys=True)
assert E["schema_version"] == "1"
assert E["integrity"] == "ok"
assert E["tables_match"]
assert E["refs_ok"]
assert E["attempts_match"] and E["notification_rows_match"] and E["turn_rows_match"]
assert E["events_match"]
conn.close()
finish()
"""

# ---- 驱动 3：新代码只读 legacy/restored 台账（行级只读断言） ----
DRIVER_READ_LEGACY_NEW = _DRIVER_COMMON + r"""
E["sqlite3_version"] = sqlite3.sqlite_version
E["oc_orch_kind"] = "new"
E["script_label"] = LABEL

from oc_orch.adapters.sqlite_db import connect, meta_get
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters import results as results_mod

""" + _TABLES + _dump_fn() + r"""
conn = connect(HOME)
ledger = Ledger(conn, HOME)
E["schema_version"] = meta_get(conn, "schema_version")
before = dump_rows(conn)
E["read_scope"] = {"tasks": [t["id"] for t in ledger.list_tasks(limit=200)],
                   "attempts": [a["id"] for a in ledger.list_attempts(limit=200)]}
E["legacy"] = {}
for aid in ("A-c1", "A-c2"):
    att = ledger.get_attempt(aid)
    task = ledger.get_task(att["task_id"])
    snap, err = results_mod.read_result_artifact(att, task)
    E["legacy"][aid] = {
        "status": att["status"],
        "turns": ledger.turns(aid),
        "deliverables": [{"target": d["target"], "status": d["status"]}
                         for d in ledger.deliverables(aid)],
        "artifact_readable": snap is not None,
        "artifact_error": err,
        "artifact_schema": (snap or {}).get("schema"),
        "artifact_protocol_version": (snap or {}).get("protocol_version"),
        "artifact_sha256_matches": bool(snap) and snap["sha256"] == sha256_file(att["result_path"]),
        "metadata_has_execution": (att["metadata"] or {}).get("execution") is not None,
        "notification_state_by_attempt": {
            n["id"]: n["state"] for n in ledger.list_notifications(limit=200)
            if n.get("attempt_id") == aid},
    }
    assert snap is not None, (aid, err)
    assert E["legacy"][aid]["artifact_sha256_matches"]
    assert E["legacy"][aid]["metadata_has_execution"] is False
E["readonly_rows_unchanged"] = json.dumps(dump_rows(conn), sort_keys=True) == json.dumps(before, sort_keys=True)
conn.close()
conn2 = connect(HOME)
E["readonly_after_reopen_rows_same"] = json.dumps(dump_rows(conn2), sort_keys=True) == json.dumps(before, sort_keys=True)
conn2.close()
assert E["schema_version"] == "1"
assert E["readonly_rows_unchanged"] is True
assert E["readonly_after_reopen_rows_same"] is True
finish()
"""

# ---- 驱动 4：新代码在 legacy 副本上显式补收集（M2/T29），旧 ACK/turn 不回写 ----
DRIVER_COLLECT_LEGACY_COPY_NEW = _DRIVER_COMMON + r"""
E["sqlite3_version"] = sqlite3.sqlite_version
E["oc_orch_kind"] = "new"
E["script_label"] = LABEL

from oc_orch.adapters.sqlite_db import connect, meta_get
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters import results as results_mod

""" + _DRIVER_FAKE + _DRIVER_CFG + r"""
conn = connect(HOME)
ledger = Ledger(conn, HOME)
E["schema_version"] = meta_get(conn, "schema_version")

def notif_map():
    return {n["id"]: {"state": n["state"], "acked_at": n["acked_at"], "dedupe_key": n["dedupe_key"]}
            for n in ledger.list_notifications(limit=200)}

turns_before = {aid: ledger.turns(aid) for aid in ("A-c1", "A-c2")}
notifs_before = notif_map()
att_l1_before = ledger.get_attempt("A-c1")

# L1：已验收尝试的 collect 必须幂等：不写任何状态/通知，仅回读原结果指针。
# facade 不透传 already_accepted，故用"尝试行字节不变 + 通知不变 + 无新通知"断言。
info1 = results_mod.collect_attempt(ledger, FakeClient([]), "A-c1", CFG, auto=False)
att_l1_after = ledger.get_attempt("A-c1")
E["L1_collect"] = {"ok": info1.get("ok"),
                   "status": att_l1_after["status"],
                   "status_unchanged": att_l1_after["status"] == att_l1_before["status"],
                   "row_unchanged": json.dumps(att_l1_after, sort_keys=True)
                   == json.dumps(att_l1_before, sort_keys=True),
                   "result_path": info1.get("result_path"),
                   "result_path_unchanged": info1.get("result_path") == att_l1_before["result_path"],
                   "notification_ids": info1.get("notification_ids") or [],
                   "notifications_unchanged": notif_map() == notifs_before,
                   "result_version": info1.get("result_version")}
assert att_l1_after["status"] == "accepted"
assert info1.get("notification_ids") == []
assert info1.get("result_path") == att_l1_before["result_path"]
assert E["L1_collect"]["row_unchanged"] is True
assert E["L1_collect"]["notifications_unchanged"] is True

# L2：写回真实完成内容 + 真实 assistant 文本，显式补 collect。F2 恢复门禁：
# 锚点输入（msg-n2-in）在窗口可见且时间早于 REST 成功上界 idle，才可收集。
a2 = ledger.get_attempt("A-c2")
report = os.path.join(a2["directory"], "out", "report.md")
E["L2_old_flat_sha_before"] = sha256_file(a2["result_path"])
with open(report, "w", encoding="utf-8") as f:
    f.write("recovered v2 content")
TS2 = 1_700_000_000_000
anchor = {"id": "msg-n2-in", "type": "user",
          "time": {"created": TS2 + 4000},
          "content": [{"type": "text", "text": "原始输入"}]}
l2sess = {"id": "S-legacy-2", "outcome": "succeeded",
          "time": {"busy": TS2 + 4000, "idle": TS2 + 9000}}
msgs = [anchor,
        {"id": "msg-l2-recover", "type": "assistant",
         "time": {"created": TS2 + 8000, "completed": TS2 + 8500},
         "content": [{"type": "text", "text": "真实完成后的有效结果文本"}]},
        {"id": "msg-l2-idle", "type": "idle", "outcome": "succeeded",
         "time": {"created": TS2 + 9000}}]
info2 = results_mod.collect_attempt(ledger, FakeClient(msgs, session=l2sess),
                                    "A-c2", CFG, auto=True)
E["L2_collect"] = {"ok": info2.get("ok"), "effective": info2.get("effective"),
                   "result_version": info2.get("result_version"),
                   "result_path": info2.get("result_path"),
                   "classification": info2.get("classification"),
                   "ready_for_verification": info2.get("ready_for_verification"),
                   "notification_ids": info2.get("notification_ids") or [],
                   "stale": info2.get("stale") or [],
                   "status_after": info2.get("status_after") or info2.get("status")}
assert info2.get("effective") is True
assert info2.get("result_version")
ver_path = info2["result_path"]
assert os.path.isfile(ver_path), ver_path
with open(ver_path, "r", encoding="utf-8") as f:
    ver_doc = json.load(f)
E["L2_version_doc"] = {"schema": ver_doc.get("schema"),
                       "protocol_version": ver_doc.get("protocol_version"),
                       "result_version": ver_doc.get("result_version"),
                       "text_full_sha256": ver_doc.get("text_full_sha256"),
                       "sha256": sha256_file(ver_path)}
assert ver_doc["schema"] == "oc-orch.result.v1"
assert ver_doc["protocol_version"] == "rv1"
assert ver_doc["result_version"] == info2["result_version"]

notifs_after = notif_map()
att2_after = ledger.get_attempt("A-c2")
old_l2 = [n for nid, n in notifs_before.items()
          if n["dedupe_key"] == "legacy-result:A-c2"][0] if True else None
old_l2_id = None
for nid, n in notifs_before.items():
    if n["dedupe_key"] == "legacy-result:A-c2":
        old_l2_id = nid
assert old_l2_id is not None
new_l2 = [n for nid, n in notifs_after.items()
          if n["dedupe_key"] == "result-ready:A-c2:" + info2["result_version"]]
E["L2_after"] = {
    "turns_unchanged": json.dumps(ledger.turns("A-c2"), sort_keys=True)
    == json.dumps(turns_before["A-c2"], sort_keys=True),
    "old_acked_unchanged": (notifs_after.get(old_l2_id) == notifs_before.get(old_l2_id)
                            and notifs_before[old_l2_id]["state"] == "acked"),
    "new_notification_pending": bool(new_l2) and new_l2[0]["state"] == "pending",
    "attempt_status": att2_after["status"],
    "result_path_is_version": att2_after["result_path"] == ver_path,
    "meta_result_version": (att2_after["metadata"] or {}).get("result", {}).get("version"),
    "old_flat_sha_after": sha256_file(os.path.join(HOME, "results", "A-c2.json")),
}
E["L2_after"]["compat_view_refreshed_not_authoritative"] = (
    E["L2_after"]["old_flat_sha_after"] != E["L2_old_flat_sha_before"])
assert E["L2_after"]["turns_unchanged"]
assert E["L2_after"]["old_acked_unchanged"]
assert E["L2_after"]["new_notification_pending"]
assert E["L2_after"]["result_path_is_version"]
assert E["L2_after"]["meta_result_version"] == info2["result_version"]
conn.close()
finish()
"""

# ---- 驱动 5：新代码完整新协议记录（封存→started→terminal→collect→版本+通知） ----
DRIVER_FRESH_NEW = _DRIVER_COMMON + r"""
E["sqlite3_version"] = sqlite3.sqlite_version
E["oc_orch_kind"] = "new"
E["script_label"] = LABEL

from oc_orch.adapters.sqlite_db import init, meta_get
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters import results as results_mod

""" + _DRIVER_FAKE + _DRIVER_CFG + r"""
conn = init(HOME)
ledger = Ledger(conn, HOME)
E["schema_version"] = meta_get(conn, "schema_version")
ts = 1_800_000_000_000
d = os.path.join(HOME, "work-n1")
os.makedirs(os.path.join(d, "out"), exist_ok=True)
dlv = os.path.join(d, "out", "deliverable.txt")
with open(dlv, "w", encoding="utf-8") as f:
    f.write("v0-baseline-content")

ledger.create_task(project="demo", title="fresh new protocol (P5)",
                   coordinator_session="ses-n1", task_id="T-n1")
ledger.create_attempt("T-n1", attempt_id="A-n1", directory=d,
                      session_id="S-new-1", model="m", generation=1,
                      status="creating", dispatch_msg_id="msg-n1-in",
                      deliverables=["out/deliverable.txt"])
ledger.record_dispatch("A-n1", "confirmed", started=True, task_status="running")

baseline = results_mod.scan_deliverable_baseline(ledger, "A-n1")
E["baseline"] = {"path": baseline.get("path"),
                 "baseline_sha256": baseline.get("baseline_sha256"),
                 "schema": baseline.get("schema")}
assert baseline.get("schema") == "oc-orch.baseline.v1"
assert baseline.get("path") and os.path.isfile(baseline["path"])

seal = ledger.seal_execution("A-n1", mode="new",
                             session_before={"idle": None, "outcome": None},
                             baseline={"path": baseline["path"],
                                       "baseline_sha256": baseline["baseline_sha256"]})
E["seal"] = {"sealed": seal["sealed"], "already": seal["already"],
             "dispatch_sealed_at": (seal["attempt"]["metadata"].get("execution")
                                    or {}).get("dispatch", {}).get("sealed_at")}
assert seal["sealed"] is True

st = ledger.record_execution_started("A-n1", started_at=ts + 1000,
                                     event_id="ev-n1-start", seq=1, source="sse")
E["start"] = {"result": st["result"], "attempt_status": st["attempt"]["status"],
              "epoch_started_at": (st["attempt"]["metadata"].get("execution") or {})
                                 .get("epoch", {}).get("started_at")}
assert st["result"] in ("accepted", "duplicate")
assert E["start"]["epoch_started_at"] == ts + 1000

te = ledger.record_terminal("A-n1", outcome="succeeded", source="sse",
                            event_id="ev-n1-end", seq=2,
                            candidate={"busy_at": ts + 1000, "ended_at": ts + 9000})
E["terminal"] = {"result": te["result"], "reason": te["reason"],
                 "attempt_status": te["attempt"]["status"],
                 "task_status": ledger.get_task("T-n1")["status"],
                 "terminal_outcome": (te["attempt"]["metadata"].get("execution") or {})
                                     .get("terminal", {}).get("outcome")}
assert te["result"] == "accepted", te
assert E["terminal"]["attempt_status"] == "terminal_succeeded"

with open(dlv, "w", encoding="utf-8") as f:
    f.write("v1-final-work-content")

msgs = [{"id": "msg-n1-in", "type": "user",
         "time": {"created": ts + 500},
         "content": [{"type": "text", "text": "输入"}]},
        {"id": "msg-n1-o1", "type": "assistant",
         "time": {"created": ts + 3000, "completed": ts + 8500},
         "content": [{"type": "text", "text": "这是本轮实质完成结果"}]}]
info = results_mod.collect_attempt(ledger, FakeClient(msgs), "A-n1", CFG, auto=True)
E["collect"] = {"ok": info.get("ok"), "effective": info.get("effective"),
                "result_version": info.get("result_version"),
                "result_path": info.get("result_path"),
                "classification": info.get("classification"),
                "ready_for_verification": info.get("ready_for_verification"),
                "notification_ids": info.get("notification_ids") or [],
                "collection_state": info.get("collection_state"),
                "collected_at": info.get("collected_at")}
assert info.get("effective") is True
assert info.get("result_version")
ver_path = info["result_path"]
assert os.path.isfile(ver_path), ver_path
with open(ver_path, "r", encoding="utf-8") as f:
    vdoc = json.load(f)
E["version_doc"] = {"schema": vdoc.get("schema"), "protocol_version": vdoc.get("protocol_version"),
                    "result_version": vdoc.get("result_version"),
                    "content_fingerprint": vdoc.get("content_fingerprint"),
                    "text_full_sha256": vdoc.get("text_full_sha256"),
                    "collected_at": vdoc.get("collected_at"),
                    "deliverables": vdoc.get("deliverables"),
                    "sha256": sha256_file(ver_path),
                    "filename": os.path.basename(ver_path)}
assert vdoc["schema"] == "oc-orch.result.v1"
assert vdoc["protocol_version"] == "rv1"
assert vdoc["result_version"] == info["result_version"]
assert vdoc["text_full_sha256"] == hashlib.sha256("这是本轮实质完成结果".encode("utf-8")).hexdigest()
assert os.path.basename(ver_path) == info["result_version"] + ".json"

att = ledger.get_attempt("A-n1")
task = ledger.get_task("T-n1")
meta = att["metadata"] or {}
E["after"] = {"attempt_status": att["status"], "task_status": task["status"],
              "meta_result_version": (meta.get("result") or {}).get("version"),
              "meta_result_path": (meta.get("result") or {}).get("path"),
              "meta_result_content_fingerprint": (meta.get("result") or {}).get("content_fingerprint"),
              "collection_state": (meta.get("collection") or {}).get("state"),
              "provenance": meta.get("deliverable_provenance"),
              "execution_terminal": (meta.get("execution") or {}).get("terminal")}
assert E["after"]["attempt_status"] == "result_collected"
assert E["after"]["task_status"] == "result_received"
assert E["after"]["meta_result_version"] == info["result_version"]
assert E["after"]["meta_result_path"] == ver_path
assert E["after"]["meta_result_content_fingerprint"] == vdoc["content_fingerprint"]
assert E["after"]["collection_state"] == "complete"

compat = os.path.join(HOME, "results", "A-n1.json")
E["compat_view"] = {"exists": os.path.isfile(compat),
                    "flat_path": compat, "version_path": ver_path,
                    "db_pointer_is_version": att["result_path"] == ver_path,
                    "flat_is_not_authoritative": att["result_path"] != compat}
assert E["compat_view"]["exists"]
assert E["compat_view"]["db_pointer_is_version"]
assert E["compat_view"]["flat_is_not_authoritative"]

nf_rows = [n for n in ledger.list_notifications(limit=200)
           if n["dedupe_key"] == "result-ready:A-n1:" + info["result_version"]]
E["notification"] = {"count": len(nf_rows), "state": nf_rows[0]["state"] if nf_rows else None,
                     "target_session": nf_rows[0]["target_session"] if nf_rows else None,
                     "payload_version": (nf_rows[0]["payload"] or {}).get("result_version") if nf_rows else None,
                     "payload_classification": (nf_rows[0]["payload"] or {}).get("classification") if nf_rows else None,
                     "payload_ready": (nf_rows[0]["payload"] or {}).get("ready_for_verification") if nf_rows else None}
assert E["notification"]["count"] == 1
assert E["notification"]["state"] == "pending"
assert E["notification"]["target_session"] == "ses-n1"
assert E["notification"]["payload_version"] == info["result_version"]
assert E["notification"]["payload_ready"] is True
assert E["notification"]["payload_classification"] == "RESULT_READY"
conn.close()
finish()
"""

# ---- 驱动 6：旧代码读新 home（先只读）+ 破坏性混用（旧 collect + 隐式 ACK） ----
DRIVER_MIX_OLD = _DRIVER_COMMON + r"""
E["sqlite3_version"] = sqlite3.sqlite_version
E["oc_orch_kind"] = "old"
E["script_label"] = LABEL

from oc_orch.adapters.sqlite_db import connect, meta_get
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters import results as results_mod

""" + _TABLES + _dump_fn() + _DRIVER_FAKE + _DRIVER_CFG + r"""
conn = connect(HOME)
ledger = Ledger(conn, HOME)
E["schema_version"] = meta_get(conn, "schema_version")
before = dump_rows(conn)

# ---- 只读部分：旧代码可读新 schema1/v1 扩展 ----
att = ledger.get_attempt("A-n1")
task = ledger.get_task("T-n1")
ver_path = att["result_path"]
snap, err = results_mod.read_result_artifact(att, task)
E["old_read_new"] = {"artifact_readable": snap is not None, "error": err,
                     "artifact_schema": (snap or {}).get("schema"),
                     "protocol_version": (snap or {}).get("protocol_version"),
                     "result_version": (snap or {}).get("result_version"),
                     "sha256_matches": bool(snap) and snap["sha256"] == sha256_file(ver_path),
                     "turns": ledger.turns("A-n1"),
                     "deliverable_status": [{"target": d["target"], "status": d["status"]}
                                            for d in ledger.deliverables("A-n1")],
                     "notification_states": {n["id"]: n["state"]
                                             for n in ledger.list_notifications(limit=200)}}
assert snap is not None, err
assert E["old_read_new"]["sha256_matches"]
E["old_readonly_rows_unchanged"] = json.dumps(dump_rows(conn), sort_keys=True) == json.dumps(before, sort_keys=True)
assert E["old_readonly_rows_unchanged"] is True

# ---- 破坏性混用：旧 collect 覆写指针 + 隐式 ACK 新协议通知 ----
meta_before = dict(att["metadata"] or {})
E["mix_before"] = {"result_path": ver_path,
                   "meta_result_path": (meta_before.get("result") or {}).get("path"),
                   "meta_result_version": (meta_before.get("result") or {}).get("version"),
                   "version_file_sha": sha256_file(ver_path)}
assert E["mix_before"]["meta_result_path"] == ver_path

msgs = [{"id": "msg-mix-rewrite", "type": "assistant",
         "time": {"created": (att["started_at"] or 0) + 1500},
         "content": [{"type": "text", "text": "旧代码 collect 覆写的新文本"}]}]
results_mod.collect_attempt(ledger, FakeClient(msgs), "A-n1", CFG, auto=False)
acked = ledger.ack_notifications_for_attempt("A-n1", actor="coordinator:collect")
att2 = ledger.get_attempt("A-n1")
meta2 = dict(att2["metadata"] or {})
flat_path = os.path.join(HOME, "results", "A-n1.json")
ver = (meta2.get("result") or {}).get("version")
ver_file = (os.path.join(HOME, "results", "A-n1", "versions",
            str(ver) + ".json") if ver else None)
nf_rows = [n for n in ledger.list_notifications(limit=200)
           if n.get("attempt_id") == "A-n1"]
E["mix_after"] = {"result_path": att2["result_path"],
                  "meta_result_version_preserved": (meta2.get("result") or {}).get("version") == ver,
                  "result_path_is_version_file": att2["result_path"] == ver_file,
                  "result_path_is_flat": att2["result_path"] == flat_path,
                  "flat_sha": sha256_file(flat_path) if os.path.isfile(flat_path) else None,
                  "version_file_still_exists": bool(ver_file) and os.path.isfile(ver_file),
                  "version_file_sha_unchanged": bool(ver_file) and os.path.isfile(ver_file)
                  and sha256_file(ver_file) == E["mix_before"]["version_file_sha"],
                  "acked_count_by_old": acked,
                  "notifications": [{k: n[k] for k in ("id", "dedupe_key", "state", "acked_at")}
                                    for n in nf_rows]}
assert att2["result_path"] != ver_file, "旧 collect 应把 result_path 指向 flat 文件"
assert att2["result_path"] == flat_path
assert att2["result_path"] == flat_path
assert E["mix_after"]["version_file_sha_unchanged"] is True
assert any(n["dedupe_key"] == "result-ready:A-n1:" + ver and n["state"] == "acked"
           for n in E["mix_after"]["notifications"]), "旧隐式 ACK 应确认新协议通知"
conn.close()
finish()
"""

# ---- 驱动 7：新代码面对混用状态——verify 拒绝，collect 以新版本修复 ----
DRIVER_MIX_REFUSE_NEW = _DRIVER_COMMON + r"""
E["sqlite3_version"] = sqlite3.sqlite_version
E["oc_orch_kind"] = "new"
E["script_label"] = LABEL

from oc_orch.adapters.sqlite_db import connect, meta_get
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters import results as results_mod
from oc_orch.domain.errors import LedgerError

""" + _DRIVER_FAKE + _DRIVER_CFG + r"""
conn = connect(HOME)
ledger = Ledger(conn, HOME)
E["schema_version"] = meta_get(conn, "schema_version")
att = ledger.get_attempt("A-n1")
E["before"] = {"result_path": att["result_path"],
               "meta_result_version": (att["metadata"] or {}).get("result", {}).get("version")}

# 1) 新 verify 正常 pass 必须拒绝混用状态（版本/路径不一致）
try:
    results_mod.prepare_verification(ledger, "A-n1", result="pass", reason="mix-verify")
    results_mod.verify_attempt(ledger, "A-n1", result="pass", reason="mix-verify")
    E["new_verify"] = {"rejected": False}
except LedgerError as exc:
    E["new_verify"] = {"rejected": True, "code": exc.code, "message": str(exc)[:160],
                       "detail": exc.detail}
except Exception as exc:
    E["new_verify"] = {"rejected": "other", "error": repr(exc)[:200]}
assert E["new_verify"].get("rejected") is True and E["new_verify"].get("code") == "RESULT_CHANGED", E["new_verify"]

# 2) 新 collect：有效候选产生新版本，修复指针（旧 ACK 不回写）
old_ver = E["before"]["meta_result_version"]
msgs = [{"id": "msg-n1-in", "type": "user",
         "time": {"created": (att["started_at"] or 0) - 500},
         "content": [{"type": "text", "text": "输入"}]},
        {"id": "msg-mix-heal", "type": "assistant",
         "time": {"created": (att["started_at"] or 0) + 1500},
         "content": [{"type": "text", "text": "混用后重收集的新有效结果"}]}]
info = results_mod.collect_attempt(ledger, FakeClient(msgs), "A-n1", CFG, auto=False)
E["new_collect"] = {"ok": info.get("ok"), "effective": info.get("effective"),
                    "result_version": info.get("result_version"),
                    "result_path": info.get("result_path"),
                    "new_version_differs": info.get("result_version") != old_ver}
assert info.get("effective") is True
assert info.get("result_version") and info["result_version"] != old_ver
att2 = ledger.get_attempt("A-n1")
E["healed"] = {"result_path_is_version": att2["result_path"] == info["result_path"],
               "meta_result_version": (att2["metadata"] or {}).get("result", {}).get("version")}
assert E["healed"]["result_path_is_version"]
assert E["healed"]["meta_result_version"] == info["result_version"]

# 3) 修复后新 verify 正常 pass 应成功（版本一致）
try:
    results_mod.prepare_verification(ledger, "A-n1", result="pass", reason="post-heal")
    vres = results_mod.verify_attempt(ledger, "A-n1", result="pass", reason="post-heal")
    E["new_verify_after_heal"] = {"ok": True,
                                  "attempt_status": vres.get("attempt", {}).get("status"),
                                  "task_status": vres.get("task", {}).get("status")}
except LedgerError as exc:
    E["new_verify_after_heal"] = {"ok": False, "code": exc.code, "message": str(exc)[:160]}
assert E["new_verify_after_heal"].get("ok") is True, E["new_verify_after_heal"]

# 4) 旧 ACK 确认过的 v1 通知不回写/不复活；v2 通知新产生
N = {n["id"]: {k: n[k] for k in ("dedupe_key", "state", "acked_at")}
     for n in ledger.list_notifications(limit=200)}
E["notifications_after"] = {
    "acked_v1_preserved": any(n["dedupe_key"] == "result-ready:A-n1:" + old_ver
                              and n["state"] == "acked" for n in N.values()),
    "pending_v2_created": any(n["dedupe_key"] == "result-ready:A-n1:" + info["result_version"]
                              and n["state"] == "pending" for n in N.values()),
}
assert E["notifications_after"]["acked_v1_preserved"]
assert E["notifications_after"]["pending_v2_created"]
conn.close()
finish()
"""

# ---- 驱动 8：同一混用状态，旧代码 verify 直接 PASS（降级接受） ----
DRIVER_MIX_DOWNGRADE_OLD = _DRIVER_COMMON + r"""
E["sqlite3_version"] = sqlite3.sqlite_version
E["oc_orch_kind"] = "old"
E["script_label"] = LABEL

from oc_orch.adapters.sqlite_db import connect, meta_get
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters import results as results_mod

conn = connect(HOME)
ledger = Ledger(conn, HOME)
E["schema_version"] = meta_get(conn, "schema_version")
att = ledger.get_attempt("A-n1")
E["state"] = {"result_path": att["result_path"], "status": att["status"]}
assert att["status"] == "result_collected"
try:
    results_mod.prepare_verification(ledger, "A-n1", result="pass", reason="old-verify-mixed")
    res = results_mod.verify_attempt(ledger, "A-n1", result="pass", reason="old-verify-mixed")
    att2 = ledger.get_attempt("A-n1")
    E["old_verify"] = {"accepted": att2["status"] == "accepted",
                       "attempt_status": att2["status"],
                       "task_status": ledger.get_task("T-n1")["status"],
                       "verification_schema": (att2.get("verification") or {}).get("schema")}
except LedgerError as exc:
    E["old_verify"] = {"accepted": False, "code": exc.code, "message": str(exc)[:160]}
assert E["old_verify"].get("accepted") is True, E["old_verify"]
conn.close()
finish()
"""

DRIVERS = {
    "build_legacy": DRIVER_BUILD_LEGACY,
    "verify_restored": DRIVER_VERIFY_RESTORED,
    "read_legacy_new": DRIVER_READ_LEGACY_NEW,
    "collect_legacy_copy_new": DRIVER_COLLECT_LEGACY_COPY_NEW,
    "fresh_new": DRIVER_FRESH_NEW,
    "mix_old": DRIVER_MIX_OLD,
    "mix_refuse_new": DRIVER_MIX_REFUSE_NEW,
    "mix_downgrade_old": DRIVER_MIX_DOWNGRADE_OLD,
}

# --------------------------------------------------------------------------
# 编排
# --------------------------------------------------------------------------


def sha256_file(p):
    with open(p, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def phase_run(name, work, src, home, extra_args=(), label=None):
    """运行一个驱动子进程；返回 {rc, envelope, log}。"""
    driver_dir = work / "drivers"
    driver_dir.mkdir(parents=True, exist_ok=True)
    driver = driver_dir / (name + ".py")
    driver.write_text(DRIVERS[name], encoding="utf-8")
    argv = [sys.executable, "-B", str(driver), str(home), label or name, *map(str, extra_args)]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(src)
    env["PYTHONNOUSERSITE"] = "1"
    env["OC_ORCH_HOME"] = str(home)
    env["TMPDIR"] = str(work)
    env["NO_PROXY"] = env["no_proxy"] = "127.0.0.1,localhost,::1"
    log_dir = work / "runlogs"
    log_dir.mkdir(parents=True, exist_ok=True)
    # 每个驱动也独立隔离服务发现；显式脚本入口与离线 runner 有相同边界。
    log_name = label or name
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserved:
        reserved.bind(("127.0.0.1", 0))
        service = work / (log_name + ".service.json")
        service.write_text(json.dumps({"hostname": "127.0.0.1",
                                      "port": reserved.getsockname()[1],
                                      "password": ""}), encoding="utf-8")
        env["OC_ORCH_SERVICE_CONFIG"] = str(service)
        proc = subprocess.run(argv, cwd=str(work), env=env, capture_output=True, text=True)
    out, err = proc.stdout, proc.stderr
    envelope = None
    marker_b = "__REHEARSE_JSON_BEGIN__"
    if marker_b in out:
        head, _, tail = out.partition(marker_b)
        tail = tail.split("__REHEARSE_JSON_END__", 1)[0].strip()
        try:
            envelope = json.loads(tail)
        except ValueError as exc:
            err += f"\n[envelope-parse] {exc}\n"
    (log_dir / (log_name + ".out.txt")).write_text(out, encoding="utf-8")
    (log_dir / (log_name + ".err.txt")).write_text(err, encoding="utf-8")
    if envelope is not None:
        (log_dir / (log_name + ".json")).write_text(
            json.dumps(envelope, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    record = {"name": name, "label": label, "home": str(home), "source": str(src),
              "rc": proc.returncode, "argv": " ".join(argv),
              "envelope_ok": envelope is not None, "envelope": envelope,
              "stderr_tail": err[-1200:]}
    (log_dir / (log_name + ".record.json")).write_text(
        json.dumps(record, ensure_ascii=False, indent=1), encoding="utf-8")
    return record


def git_info():
    def g(*args):
        return subprocess.run(["git", *args], cwd=str(REPO), capture_output=True,
                              text=True).stdout.strip()
    return {"base": BASE, "head": g("rev-parse", "HEAD"),
            "status_dirty": bool(g("status", "--porcelain"))}


def src_snapshot(src):
    """记录被验证的 src/oc_orch 全部 python 文件 sha256（钉住实际代码版本）。

    所有阶段运行同一份源码副本，摘要对应实际加载的文件。
    """
    out = {}
    root = src / "oc_orch"
    if root.is_dir():
        for p in sorted(root.rglob("*.py")):
            rel = str(p.relative_to(src))
            out[rel] = sha256_file(p)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--baseline-repo", type=Path,
                    default=os.environ.get("OC_ORCH_MIGRATION_BASELINE_REPO", REPO),
                    help="包含固定旧版本的本地 Git 仓库")
    ap.add_argument("--work-dir", default=None)
    ap.add_argument("--evidence", default=None)
    ap.add_argument("--keep", action="store_true", help="保留工作目录（默认保留）")
    args = ap.parse_args()

    work = Path(args.work_dir) if args.work_dir else Path(
        tempfile.mkdtemp(prefix="rehearse-completion-migration-", dir="/tmp/opencode"))
    work.mkdir(parents=True, exist_ok=True)
    evidence = Path(args.evidence) if args.evidence else DEFAULT_EVIDENCE
    evidence.mkdir(parents=True, exist_ok=True)
    homes = work / "homes"
    homes.mkdir(parents=True, exist_ok=True)

    phases = []
    failures = []

    def run_phase(name, src, home, extra=(), label=None):
        rec = phase_run(name, work, src, home, extra_args=extra, label=label)
        phases.append(rec)
        if rec["rc"] != 0 or not rec["envelope_ok"]:
            failures.append(rec["name"])
        return rec

    # ---- 0. 提取旧代码（只读） ----
    old_src = work / "old" / "src"
    old_src.parent.mkdir(parents=True, exist_ok=True)
    if not old_src.exists():
        tar = subprocess.run(
            ["git", "archive", "--format=tar", BASE, "src"],
            cwd=args.baseline_repo, capture_output=True)
        if tar.returncode != 0:
            print(f"git archive 失败：{tar.stderr.decode()[:400]}", file=sys.stderr)
            return 2
        subprocess.run(["tar", "-x", "-C", str(old_src.parent)], input=tar.stdout, check=True)
    new_src = work / "new" / "src"
    if new_src.exists():
        shutil.rmtree(new_src)
    shutil.copytree(REPO / "src", new_src,
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))

    home_legacy = homes / "legacy"
    home_restored = homes / "restored"
    home_legacy_copy = homes / "legacy-copy"
    home_fresh = homes / "fresh-new"
    home_mix_a = homes / "fresh-new-mix-a"
    home_mix_b = homes / "fresh-new-mix-b"
    for h in (home_legacy, home_restored, home_legacy_copy, home_fresh,
              home_mix_a, home_mix_b):
        if h.exists():
            shutil.rmtree(h)

    # ---- P1：旧代码构建 legacy + WAL 直拷/backup 对照 ----
    r1 = run_phase("build_legacy", old_src, home_legacy)
    e1 = r1["envelope"]

    # ---- P2：一致备份 + 结果目录备份 + restore 验证 ----
    snap = work / "snapshots" / "full"
    if snap.exists():
        shutil.rmtree(snap)
    snap.mkdir(parents=True, exist_ok=True)
    bdb = snap / "ledger-backup.db"
    db_path = home_legacy / "ledger.db"
    src_conn = sqlite3.connect(str(db_path))
    dst_conn = sqlite3.connect(str(bdb))
    src_conn.backup(dst_conn)
    dst_conn.close()
    src_conn.close()
    results_src = home_legacy / "results"
    results_dst = snap / "results"
    if results_src.is_dir():
        shutil.copytree(results_src, results_dst)
    if e1:
        manifest = {
            "schema_version": e1.get("schema_version"),
            "sqlite3_version": e1.get("sqlite3_version"),
            "tables": e1.get("tables") or {},
            "refs": e1.get("refs") or [],
            "files": e1.get("result_files") or {},
            "attempts": e1.get("attempts") or {},
            "notification_rows": e1.get("notification_rows") or {},
            "turn_rows": e1.get("turn_rows") or {},
            "event_rows": e1.get("event_rows") or [],
            "db_backup_sha256": sha256_file(bdb),
            "captured_at_ms": int(time.time() * 1000),
        }
        (snap / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
        # restore：db + results 恢复进独立 home
        home_restored.mkdir(parents=True, exist_ok=True)
        shutil.copy2(bdb, home_restored / "ledger.db")
        shutil.copytree(results_dst, home_restored / "results")
        r2 = run_phase("verify_restored", old_src, home_restored,
                       extra=(snap / "manifest.json",), label="restored-old")
        r3 = run_phase("read_legacy_new", new_src, home_restored, label="restored-new")
        p2_ok = (r2["rc"] == 0 and r3["rc"] == 0)
    else:
        manifest = None
        p2_ok = False

    # ---- P3：新代码只读旧 legacy ----
    r4 = run_phase("read_legacy_new", new_src, home_legacy, label="legacy-new")

    # ---- P4：legacy 副本上显式补收集（T29/M2） ----
    shutil.copytree(home_legacy, home_legacy_copy)
    r5 = run_phase("collect_legacy_copy_new", new_src, home_legacy_copy)

    # ---- P5：新代码完整新协议记录 ----
    r6 = run_phase("fresh_new", new_src, home_fresh)

    # ---- P6：版本混用 / 降级边界 ----
    shutil.copytree(home_fresh, home_mix_a)
    shutil.copytree(home_fresh, home_mix_b)
    r7a = run_phase("mix_old", old_src, home_mix_a, label="mix-a-mixold")
    r8a = run_phase("mix_refuse_new", new_src, home_mix_a, label="mix-a-refuse")
    r7b = run_phase("mix_old", old_src, home_mix_b, label="mix-b-mixold")
    r8b = run_phase("mix_downgrade_old", old_src, home_mix_b, label="mix-b-downgrade")

    # ---- 主侧断言（真实数据与哈希） ----
    asserts = []
    ok = True

    def chk(cond, msg):
        nonlocal ok
        asserts.append({"ok": bool(cond), "msg": msg})
        if not cond:
            ok = False

    sqlite_ver = sqlite3.sqlite_version
    chk(e1 is not None, "P1 envelope 存在")
    if e1:
        chk(e1["schema_version"] == "1", "P1 schema_version == 1")
        chk(e1["journal_mode"] in ("wal", "WAL"), "P1 journal_mode == wal")
        chk(e1["sqlite3_version"] == sqlite_ver, "P1 sqlite3 版本与主进程一致")
        chk(e1["backup_marker_count"] == 1, "backup API 快照含未 checkpoint marker")
        if e1.get("raw_copy_ok"):
            chk(e1["raw_copy_marker_count"] == 0, "直接 copy 活动主文件丢失未 checkpoint marker")
        else:
            chk(bool(e1.get("raw_copy_error")), "直接 copy 活动主文件不可作为完整备份")
        chk(e1["backup_integrity"] == "ok", "backup 快照 integrity ok")
        chk(e1["L1"]["attempt_status"] == "accepted" and e1["L1"]["notification"]["state"] == "acked",
            "L1 accepted+ACK 由旧代码建好")
        chk(e1["L2"]["result_text_len"] == 0 and e1["L2"]["notification"]["state"] == "acked",
            "L2 空结果已 ACK（坏 turn 形态）")
        chk(manifest is not None, "backup manifest 生成")
        if manifest:
            chk(manifest["db_backup_sha256"] == sha256_file(bdb), "backup db 哈希已记录")
    chk(r2["rc"] == 0 and r2.get("envelope", {}).get("refs_ok") is True, "restore 后（旧代码）引用一致")
    chk(r3["rc"] == 0 and r3.get("envelope", {}).get("readonly_rows_unchanged") is True,
        "restore 后（新代码）只读且一致")
    chk(r4["rc"] == 0 and r4.get("envelope", {}).get("readonly_rows_unchanged") is True,
        "新代码只读旧数据（行级不变）且旧 ACK/accepted/坏 turn 不回写")
    chk(r5["rc"] == 0 and r5.get("envelope", {}).get("L2_after", {}).get("turns_unchanged"),
        "legacy 副本补收集：历史坏 turn 不回写")
    chk(r5["rc"] == 0 and r5.get("envelope", {}).get("L2_after", {}).get("old_acked_unchanged"),
        "legacy 副本补收集：旧 ACK 不回写")
    chk(r6["rc"] == 0 and r6.get("envelope", {}).get("collect", {}).get("effective") is True,
        "新协议 collect 有效且产生版本")
    r6e = r6.get("envelope", {})
    chk(r6["rc"] == 0 and bool(r6e.get("notification"))
        and r6e.get("notification", {}).get("payload_ready") is True
        and r6e.get("notification", {}).get("payload_classification") == "RESULT_READY",
        "新协议通知 payload 版本/就绪/分类正确")
    r8a_e = r8a.get("envelope", {})
    chk(r8a["rc"] == 0 and r8a_e.get("new_verify", {}).get("code") == "RESULT_CHANGED",
        "混用状态新 verify 拒绝（RESULT_CHANGED）")
    chk(r8a["rc"] == 0 and r8a_e.get("healed", {}).get("result_path_is_version") is True,
        "混用状态新 collect 以新版本修复指针")
    chk(r8a["rc"] == 0 and r8a_e.get("notifications_after", {}).get("acked_v1_preserved") is True,
        "旧 ACK 确认的 v1 通知不回写不复活")
    r7b_e = r7b.get("envelope", {})
    r8b_e = r8b.get("envelope", {})
    chk(r7b["rc"] == 0 and r7b_e.get("mix_after", {}).get("result_path_is_flat") is True,
        "旧 collect 会把新协议指针改写为 flat 文件（破坏性）")
    chk(r7b["rc"] == 0 and r7b_e.get("mix_after", {}).get("version_file_sha_unchanged") is True,
        "不可变版本文件未被旧代码改写")
    chk(r8b["rc"] == 0 and r8b_e.get("old_verify", {}).get("accepted") is True,
        "同一混用状态旧 verify 直接接受（降级不能维持新约束）")

    # ---- 证据落盘 ----
    run_records = []
    for p in phases:
        run_records.append({k: p[k] for k in ("name", "label", "rc", "argv",
                                              "envelope_ok", "stderr_tail")})
    assembly = {
        "generated_at_ms": int(time.time() * 1000),
        "git": git_info(),
        "src_snapshot": src_snapshot(new_src),
        "python": sys.version.split()[0],
        "sqlite3": sqlite_ver,
        "work_dir": str(work),
        "old_code_src": str(old_src),
        "new_code_src": str(new_src),
        "phases": run_records,
        "asserts": asserts,
        "all_asserts_passed": ok,
        "failures": failures,
        "env_note": "各驱动的 OC_ORCH_HOME/TMPDIR 指向演练目录，服务配置为占用但不监听的 loopback；全部新代码阶段加载同一源码副本",
    }
    if e1:
        assembly["p1"] = e1
    if r2.get("envelope"):
        assembly["p2_restored_old"] = r2["envelope"]
    if r3.get("envelope"):
        assembly["p2_restored_new"] = r3["envelope"]
    if r4.get("envelope"):
        assembly["p3_legacy_new"] = r4["envelope"]
    if r5.get("envelope"):
        assembly["p4_legacy_recovery"] = r5["envelope"]
    if r6.get("envelope"):
        assembly["p5_fresh_new"] = r6["envelope"]
    if r7a.get("envelope"):
        assembly["p6_mixa_mixold"] = r7a["envelope"]
    if r8a.get("envelope"):
        assembly["p6_mixa_refuse"] = r8a["envelope"]
    if r7b.get("envelope"):
        assembly["p6_mixb_mixold"] = r7b["envelope"]
    if r8b.get("envelope"):
        assembly["p6_mixb_downgrade"] = r8b["envelope"]

    (evidence / "rehearse-evidence.json").write_text(
        json.dumps(assembly, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    (evidence / "rehearse-summary.txt").write_text(
        render_summary(assembly, e1, r8a.get("envelope"), r8b.get("envelope")),
        encoding="utf-8")
    replay = render_replay(phases)
    (evidence / "replay-commands.txt").write_text(replay, encoding="utf-8")
    (evidence / "README.md").write_text(render_readme(assembly, ok), encoding="utf-8")
    driver_out = evidence / "drivers"
    driver_out.mkdir(parents=True, exist_ok=True)
    for name, src in DRIVERS.items():
        (driver_out / (name + ".py")).write_text(src, encoding="utf-8")

    # 证据文件 sha256 清单
    manifest_hashes = {}
    for p in sorted(evidence.rglob("*")):
        if p.is_file():
            manifest_hashes[str(p.relative_to(evidence))] = sha256_file(p)
    (evidence / "manifest-sha256.json").write_text(
        json.dumps(manifest_hashes, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")

    print(f"work_dir: {work}")
    print(f"evidence: {evidence}")
    print(f"all_asserts_passed: {ok}")
    print(f"asserts: {sum(1 for a in asserts if a['ok'])}/{len(asserts)}")
    if failures:
        print("phases with failures: " + ", ".join(failures))
    return 0 if (ok and not failures) else 1


def render_replay(phases):
    lines = ["# 可复现命令（按序执行；exit 记录在 evidence.json）", ""]
    for p in phases:
        lines.append(f"## {p['name']} [{p['label'] or ''}] rc={p['rc']}")
        lines.append("    " + p["argv"])
        lines.append("")
    lines.append("# 手工复现备份/恢复主进程步骤")
    lines.append("python3 scripts/rehearse_completion_migration.py")
    return "\n".join(lines)


def render_summary(assembly, e1, refuse_env, downgrade_env):
    L = []
    A = L.append
    A("M3 演练摘要（旧数据读取 / 一致备份 / 版本混用 / 回滚边界）")
    A("")
    A(f"- git base={assembly['git']['base'][:12]} head={assembly['git']['head'][:12]}"
      f" dirty={assembly['git']['status_dirty']}")
    A(f"- python={assembly['python']} sqlite3={assembly['sqlite3']}")
    A(f"- work_dir={assembly['work_dir']}")
    A("")
    A("【备份】")
    if e1:
        A(f"- schema_version={e1.get('schema_version')} journal_mode={e1.get('journal_mode')}")
        A(f"- 未 checkpoint 写：WAL={e1.get('wal_bytes')} bytes, marker={e1.get('marker_nid')}")
        A(f"- 直接 copy 活动主文件: ok={e1.get('raw_copy_ok')} "
          f"marker_count={e1.get('raw_copy_marker_count')} "
          f"error={e1.get('raw_copy_error') or '-'}")
        A(f"- backup API: marker_count={e1.get('backup_marker_count')} "
          f"integrity={e1.get('backup_integrity')} sha={e1.get('backup_sha256')[:16]}...")
    A("")
    A("【restore 引用一致】")
    for key in ("p2_restored_old", "p2_restored_new"):
        env = assembly.get(key) or {}
        if env:
            A(f"- {key}: schema={env.get('schema_version')} refs_ok={env.get('refs_ok')} "
              f"tables_match={env.get('tables_match')} "
              f"readonly={env.get('readonly_rows_unchanged')}")
    A("")
    A("【读兼容】")
    env = assembly.get("p3_legacy_new") or {}
    if env:
        for aid, info in (env.get("legacy") or {}).items():
            A(f"- 新代码读旧数据 {aid}: status={info.get('status')} "
              f"artifact_readable={info.get('artifact_readable')} "
              f"execution_meta={info.get('metadata_has_execution')} "
              f"protocol={info.get('artifact_protocol_version')}")
        A(f"- 行级只读不变={env.get('readonly_rows_unchanged')} "
          f"重开仍一致={env.get('readonly_after_reopen_rows_same')}")
    env = assembly.get("p6_mixa_mixold") or {}
    if env:
        ro = env.get("old_read_new") or {}
        A(f"- 旧代码读新 v1 扩展: artifact_readable={ro.get('artifact_readable')} "
          f"schema={ro.get('artifact_schema')} protocol={ro.get('protocol_version')} "
          f"sha256_matches={ro.get('sha256_matches')} 只读行不变={env.get('old_readonly_rows_unchanged')}")
    A("")
    A("【版本混用 / 降级】")
    env = assembly.get("p6_mixa_refuse") or {}
    if env:
        A(f"- 混用状态新 verify: rejected={env.get('new_verify', {}).get('rejected')} "
          f"code={env.get('new_verify', {}).get('code')}")
        A(f"- 混用状态新 collect: effective={env.get('new_collect', {}).get('effective')} "
          f"新版本={env.get('new_collect', {}).get('result_version')} "
          f"修复指针={env.get('healed', {}).get('result_path_is_version')}")
        A(f"- 旧 ACK 的 v1 通知保留={env.get('notifications_after', {}).get('acked_v1_preserved')} "
          f"v2 待发={env.get('notifications_after', {}).get('pending_v2_created')}")
    env = assembly.get("p6_mixb_downgrade") or {}
    if env:
        A(f"- 同一混用状态旧 verify: accepted={env.get('old_verify', {}).get('accepted')} "
          f"verification_schema={env.get('old_verify', {}).get('verification_schema')}")
    A("")
    A("结论：schema 恒为 1、旧代码可读 ≠ 无损回滚。混跑必须被运行期拒绝（新 verify "
      "RESULT_CHANGED）或经新 collect 修复；回滚需暂停自动复用派单并人工对账。")
    return "\n".join(L)


def render_readme(assembly, ok):
    return f"""---
status: completed-rehearsal
created: 2026-11-08
updated: 2026-11-08
author: "AI（M3 离线数据/备份/混用演练子代理）"
tags: [migration, backup, rollback, compat, completion-integrity]
baseline: 9ffe2477ebd030d50d00f1a3e94ee3bdc875674f
scope: "仅本 migration 子目录；不改业务代码；不碰生产台账"
---

# M3 演练：旧数据离线兼容 / SQLite 一致备份 / 版本混用说明与回滚演练

本目录是 [fix-attempt-completion-integrity](../../../openspec/changes/fix-attempt-completion-integrity/design.md)
M2/M3 与 [verification T36](../../../openspec/changes/fix-attempt-completion-integrity/verification.md)、
[tasks 7.3](../../../openspec/changes/fix-attempt-completion-integrity/tasks.md) 的独立离线演练证据。
只读仓库源码；固定点旧代码经 `git archive` 提取到临时目录执行；全部台账位于
`/tmp/opencode` 临时目录；未触碰生产 `OC_ORCH_HOME`、未调用真实服务、未修改业务代码。

## 环境与依据

- 基点 `9ffe247` 从指定的历史仓库读取。当前版本与 Python/SQLite 版本见
  `rehearse-evidence.json`。
- 复现入口：[rehearse_completion_migration.py](../../../scripts/rehearse_completion_migration.py)。
  运行 `python3 scripts/rehearse_completion_migration.py` 或按 `replay-commands.txt`。

## 演练内容（每阶段断言真实数据/哈希）

| 阶段 | 驱动 | 验证什么 |
|---|---|---|
| P1 | `build_legacy.py`（旧代码） | 构建最小 legacy schema1：任务/尝试/accepted/ACK/旧结果/历史坏 turn；**未 checkpoint 写**存在时，直接 copy 活动主文件丢失写入，backup API 一致快照完整（integrity ok） |
| P2 | 主进程 + `verify_restored.py`（旧）+ `read_legacy_new.py`（新） | backup API 备份台账 + 结果目录，restore 后表计数、result_path 引用文件与哈希、ACK/accepted/hist 坏 turn/事件完全一致 |
| P3 | `read_legacy_new.py`（新代码） | 新代码只读旧数据：行级 dump 前=后，旧 ACK/accepted/坏 turn 不回写；旧 flat 结果可读、protocol 为空（legacy） |
| P4 | `collect_legacy_copy_new.py`（新代码，在副本上） | T29/M2 显式补收集：产生 `results/<A>/versions/<ver>.json` 新版本与新通知；历史坏 turn/旧 ACK 行不变；flat 视图仅作缓存刷新 |
| P5 | `fresh_new.py`（新代码） | 完整新协议记录：封存 execution → started → terminal → collect → `oc-orch.result.v1`/rv1 不可变版本 + `RESULT_READY` 通知；fingerprint/版本/文件哈希相互一致 |
| P6 | `mix_old.py`（旧）→ `mix_refuse_new.py`（新）→ `mix_downgrade_old.py`（旧） | 版本混用边界：旧代码能读新 v1 扩展（只读不变）；旧 collect+隐式 ACK 会覆写指针并提前确认新协议通知；新 verify 以 `RESULT_CHANGED` 拒绝混用状态，新 collect 以新版本修复；同一混用状态旧 verify 直接 PASS（降级不能维持新约束） |

## 结论与边界

1. **备份**：活动 WAL 台账的主文件直接 copy（不含 `-wal`）不是完整备份；必须用
   SQLite backup API 获取一致快照，并同时备份被引用的 `results/` 目录；restore 后
   已断言 result_path 引用文件存在且哈希一致。
2. **读兼容**：schema 恒为 `1`；新旧代码都能打开同一台账。新代码只读旧数据、旧代码
   只读新 schema1/v1 扩展均为行级只读（dump 前后一致），且旧代码读取新版本文件
   （`oc-orch.result.v1`）成功、哈希一致。
3. **版本混用必须拒绝/修复，回滚绝非无损**：schema1 不变只保证"可读"。实测旧 collect
   会覆写 result_path/result_collected_at 并隐式 ACK 新协议通知；新 verify 对混用状态
   返回 `RESULT_CHANGED`，需新 collect 产生新版本修复；已被旧代码 ACK 的 v1 通知不回写、
   不复活。回滚到旧代码 = 静默丢失版本/来源/执行证据门禁（同一状态旧 verify 直接通过）
   → 必要回滚必须保存切换后新增事实、暂停自动复用派单并人工对账，不得用旧备份覆盖
   当前台账。
4. **范围**：本子代理只做 M3 数据/备份/混用演练；M2 的 legacy 业务桥接（6.2 同内容通知
   桥接等）与 B/C 业务代码验收归主协调者。`all_asserts_passed = {ok}`；失败阶段见
   `rehearse-evidence.json` 的 `failures`/`asserts`。

## 文件

- `rehearse-evidence.json`：全部阶段信封、断言、命令与 exit、git/python/sqlite 版本、
  `src_snapshot`（被验证的 src/oc_orch 各文件 sha256）、哈希。
- `rehearse-summary.txt`：人类可读摘要（含直拷/backup 计数、混用/降级限制）。
- `replay-commands.txt`：逐阶段真实命令（含 exit）。
- `drivers/*.py`：八个驱动源码快照（与仓库脚本内嵌一致）。
- `manifest-sha256.json`：本目录全部文件 sha256 清单。
- 运行日志/信封见 `work_dir`（记录在 evidence.json）下 `runlogs/`。
- 说明：新代码为未提交工作区修复，`src_snapshot` 钉住各阶段共同加载的源码副本哈希；
   工作区代码改变后需按脚本重跑以重新取证。
""".format(ok=ok)


if __name__ == "__main__":
    sys.exit(main())

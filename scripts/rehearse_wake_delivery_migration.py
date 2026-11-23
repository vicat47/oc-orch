#!/usr/bin/env python3
"""隔离演练真实旧实现→schema2→旧写入口拒绝→一致快照恢复；无服务调用。"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile


BUILD_V1 = r'''
import json, sys
from oc_orch import SCHEMA_VERSION
from oc_orch.db import init
from oc_orch.ledger import Ledger
assert SCHEMA_VERSION == 1
conn = init(sys.argv[1]); ledger = Ledger(conn, sys.argv[1])
ledger.create_task(project="migration", title="legacy", task_id="T-legacy")
ids = []
for i in range(2):
    n = ledger.enqueue_notification(kind="result_ready", target_session="ses_legacy",
        dedupe_key="legacy-" + str(i), payload={"summary": "legacy"})
    ledger.mark_notification_sent(n["id"], message_id="msg_shared_legacy",
        next_attempt_at=9999999999999, owner="legacy")
    ids.append(n["id"])
acked = ledger.enqueue_notification(kind="result_ready", target_session="ses_legacy",
    dedupe_key="acked", payload={"summary": "acked"})
ledger.ack_notifications([acked["id"]], actor="legacy-operator")
unrouted = ledger.enqueue_notification(kind="result_ready", target_session=None,
    dedupe_key="unrouted", payload={"summary": "unrouted"})
ledger.mark_notification_sent(unrouted["id"], message_id="msg_unrouted",
    next_attempt_at=9999999999999, owner="legacy")
conn.close()
print(json.dumps({"schema": SCHEMA_VERSION, "shared_members": sorted(ids),
    "acked": acked["id"], "unrouted": unrouted["id"]}))
'''

UPGRADE = r'''
import json, sys
from oc_orch import SCHEMA_VERSION
from oc_orch.db import init
assert SCHEMA_VERSION == 2
conn = init(sys.argv[1])
version = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0]
rows = [dict(r) for r in conn.execute("SELECT * FROM session_inputs")]
assert version == "2" and len(rows) == 1
row = rows[0]
assert row["admission_state"] == "unknown" and row["outstanding"] == 0
assert row["session_id"] == "ses_legacy" and row["message_id"] == "msg_shared_legacy"
assert row["delivered_at"] is None
print(json.dumps({"schema": version, "legacy_members": sorted(json.loads(row["notification_ids"])),
    "legacy_state": row["admission_state"], "outstanding": row["outstanding"],
    "integrity": conn.execute("PRAGMA integrity_check").fetchone()[0]}))
conn.close()
'''

REJECT_OLD = r'''
import json, sys
from oc_orch import SCHEMA_VERSION
from oc_orch.db import init
assert SCHEMA_VERSION == 1
try:
    conn = init(sys.argv[1])
except RuntimeError as exc:
    print(json.dumps({"old_declared_schema": SCHEMA_VERSION, "rejected": True,
                      "error": str(exc)}))
else:
    conn.close()
    raise AssertionError("old write entry accepted schema2")
'''

RESTORED_V1 = r'''
import json, sys
from oc_orch import SCHEMA_VERSION
from oc_orch.db import init
assert SCHEMA_VERSION == 1
conn = init(sys.argv[1])
assert conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0] == "1"
print(json.dumps({"schema": 1, "notifications": conn.execute(
    "SELECT COUNT(*) FROM notifications").fetchone()[0],
    "integrity": conn.execute("PRAGMA integrity_check").fetchone()[0]}))
conn.close()
'''


def dump(home: Path, *, include_inputs: bool = False) -> dict:
    conn = sqlite3.connect(str(home / "ledger.db"))
    conn.row_factory = sqlite3.Row
    try:
        tables = ["tasks", "attempts", "turns", "deliverables", "notifications"]
        if include_inputs:
            tables += ["session_inputs", "meta", "audit"]
        return {t: [dict(r) for r in conn.execute(f"SELECT * FROM {t} ORDER BY rowid")]
                for t in tables}
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-source", type=Path, required=True,
                        help="真实schema1包源根（含oc_orch），只读取源，不读取其台账")
    parser.add_argument("--new-source", type=Path,
                        default=Path(__file__).resolve().parents[1] / "src")
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=False)
    phases = []
    with tempfile.TemporaryDirectory(prefix="wake-migration-", dir="/tmp/opencode") as tmp:
        work = Path(tmp)
        home = work / "legacy"

        def run(label: str, source: Path, target: Path, code: str) -> dict:
            env = {**os.environ, "PYTHONPATH": str(source.resolve()),
                   "OC_ORCH_HOME": str(target), "PYTHONDONTWRITEBYTECODE": "1",
                   "OC_ORCH_SERVICE_CONFIG": str(work / "unused-service.json")}
            proc = subprocess.run([sys.executable, "-B", "-c", code, str(target)],
                                  cwd=work, env=env, capture_output=True, text=True)
            (args.evidence / f"{label}.log").write_text(proc.stdout + proc.stderr)
            record = {"phase": label, "source": str(source), "returncode": proc.returncode}
            phases.append(record)
            if proc.returncode:
                raise RuntimeError(f"{label} failed: {proc.stderr[-1000:]}")
            record["result"] = json.loads(proc.stdout)
            return record["result"]

        try:
            old = run("build-v1", args.old_source, home, BUILD_V1)
            before = dump(home)
            backup = work / "consistent-v1.db"
            src = sqlite3.connect(str(home / "ledger.db"))
            dst = sqlite3.connect(str(backup))
            try:
                src.backup(dst)
            finally:
                dst.close()
                src.close()
            upgraded = run("upgrade-v2", args.new_source, home, UPGRADE)
            assert upgraded["legacy_members"] == old["shared_members"]
            assert upgraded["integrity"] == "ok" and dump(home) == before
            v2_before = dump(home, include_inputs=True)
            run("old-writer-rejected", args.old_source, home, REJECT_OLD)
            assert dump(home, include_inputs=True) == v2_before
            restored = work / "restored"
            restored.mkdir()
            shutil.copy2(backup, restored / "ledger.db")
            report = run("restore-consistent-v1", args.old_source, restored, RESTORED_V1)
            assert report["integrity"] == "ok" and dump(restored) == before
            result = {"ok": True, "scope": "isolated source/SQLite only; no production rollback or HTTP",
                      "legacy_user_rows_unchanged": True, "old_writer_rejected_without_row_changes": True,
                      "consistent_snapshot_restored": True, "phases": phases,
                      "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
        except Exception as exc:
            result = {"ok": False, "error": str(exc), "phases": phases}
    (args.evidence / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

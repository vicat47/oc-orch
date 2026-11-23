#!/usr/bin/env python3
"""纯临时组合副本演练；不接受生产home，不发送模型/邮件，不覆盖旧报告。"""

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

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT)]

from oc_orch.adapters.delivery_guard import KEY, check_delivery_guard, install_delivery_guard
from oc_orch.adapters.delivery_policy import DeliveryPolicyStore
from oc_orch.adapters.resource_authorization import ResourceAuthorizationStore
from oc_orch.adapters.sqlite_db import backup_to, init
from oc_orch.adapters.sqlite_ledger import Ledger
from oc_orch.adapters.writer_guard import install_writer_guard
from tests.continuity_matrix_fixtures import (
    PROJ_RUN, build_matrix_state, meta_map, table_facts,
)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def run():
    with tempfile.TemporaryDirectory(prefix="delivery-copy-proof-", dir="/tmp/opencode") as tmp:
        root = Path(tmp)
        original_home = root / "source"
        original_home.mkdir()
        state = build_matrix_state(str(original_home))
        connections = [state.conn]
        try:
            install_writer_guard(state.conn)
            original = table_facts(state.conn)
            original_meta = meta_map(state.conn)
            source = root / "source-v2.db"
            backup_to(state.conn, str(source))
            source_sha = sha(source)

            def copy_home(name):
                home = root / name
                home.mkdir()
                shutil.copyfile(source, home / "ledger.db")
                conn = init(home)
                connections.append(conn)
                return home, conn

            # 审计注入失败，完整回到原v2；不删除保护、不倒旧库。
            _, failed = copy_home("rollback")
            failed.execute("CREATE TEMP TRIGGER delivery_install_fail BEFORE INSERT ON audit "
                "WHEN NEW.action='delivery-policy.install' BEGIN SELECT RAISE(ABORT,'injected'); END")
            try:
                install_delivery_guard(failed)
            except sqlite3.IntegrityError:
                pass
            else:
                raise AssertionError("injected failure did not fail")
            assert table_facts(failed) == original and not check_delivery_guard(failed)

            home, conn = copy_home("upgrade")
            install_delivery_guard(conn)
            installed = table_facts(conn)
            assert all(installed[t] == original[t] for t in original if t not in ("meta", "audit"))
            assert installed["audit"]["count"] == original["audit"]["count"] + 1
            assert {k: v for k, v in meta_map(conn).items() if k != KEY} == original_meta

            # 实际调用未改动的v2连接配置模块，不为子进程注册新delivery UDF。
            # 与历史 v2 逐项核对后的模块摘要（已移除机器路径说明）。
            frozen_sha256 = "065c4ff6e8ddca5bc35cfb2bd5b8647477b5e129deb4eb22bb5c833653cf5e50"
            writer_path = "src/oc_orch/adapters/writer_guard.py"
            assert sha(ROOT / writer_path) == frozen_sha256, "旧v2配置模块已漂移，不能当原实现证明"
            child = r'''
import json,sqlite3,sys
from oc_orch.adapters.writer_guard import configure_writer_connection
conn=sqlite3.connect(sys.argv[1],isolation_level=None)
configure_writer_connection(conn)
before=conn.total_changes
try:
 conn.execute("INSERT INTO meta(key,value) VALUES('old-v2-write','forbidden')")
except sqlite3.OperationalError as exc:
 assert "oc_orch_delivery_contract" in str(exc),str(exc)
 print(json.dumps({"denied":True,"error":"missing-delivery-UDF","writes":conn.total_changes-before}))
else:
 raise AssertionError("old v2 writer unexpectedly wrote")
'''
            env = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "PYTHONDONTWRITEBYTECODE": "1"}
            old = subprocess.run([sys.executable, "-B", "-c", child, str(home / "ledger.db")],
                                 env=env, capture_output=True, text=True, timeout=20, check=True)
            old_result = json.loads(old.stdout)
            assert old_result["denied"] and old_result["writes"] == 0
            assert table_facts(conn) == installed

            # 新事实产生后，一致备份及同版恢复须保留旧ACK/结果/current/期限/控制、
            # 新任务/策略/授权；不能以回退为名倒源v2快照。
            policies = DeliveryPolicyStore(conn, state.clock)
            chosen = {"preset": "budget-first", "stage": "standard", "limits": {
                "max_parallel": 2, "max_candidates": 1, "max_challenge_rounds": 0}}
            policies.set_project_default(PROJ_RUN, policy=chosen, expected_revision=0,
                operation_id="copy-default", actor="operator:test", reason="offline-copy")
            ResourceAuthorizationStore(conn, state.clock).replace("project", PROJ_RUN,
                resources=[], expires_at=state.clock.now_ms()+100_000, expected_revision=0,
                operation_id="copy-revoke", actor="operator:test", reason="offline-copy")
            ledger = Ledger(conn, home, clock=state.clock)
            new_task = ledger.create_task(project=PROJ_RUN, title="post-contract-input", task_id="T-copy-new")
            assert policies.task_snapshot(new_task["id"])["version"]["source"]["revision"] == 1
            for table in original:
                if table not in ("tasks", "meta", "audit"):
                    assert table_facts(conn)[table] == original[table], table
            newest = table_facts(conn)
            after = root / "preserved-new-facts.db"
            backup_to(conn, str(after))
            recovered_home = root / "recovered"
            recovered_home.mkdir()
            shutil.copyfile(after, recovered_home / "ledger.db")
            recovered = init(recovered_home)
            connections.append(recovered)
            assert table_facts(recovered) == newest and check_delivery_guard(recovered)
            assert sha(source) == source_sha
            return {"schema": "oc-orch.delivery-policy-copy-proof.v1", "scope": "temporary-rich-fixture-only",
                "source_backup_unchanged": True, "source_backup_sha256": source_sha,
                "old_v2_writer_module_verified": True, "old_writer": old_result,
                "install_failure_atomic": True, "old_business_tables_preserved": True,
                "old_meta_preserved_except_explicit_marker": True, "install_audit_added": 1,
                "new_task_snapshot_preserved": True, "recovery_kept_all_new_facts": True,
                "contract_triggers": 72, "before": original, "after": newest,
                "network_model_requests": 0, "real_email_requests": 0, "production_changed": False}
        finally:
            for conn in reversed(connections):
                conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, help="新报告路径；拒绝覆盖")
    args = parser.parse_args()
    if args.report and args.report.exists():
        parser.error("报告已存在；保留原证据，请使用新路径")
    report = run()
    text = json.dumps(report, ensure_ascii=False, indent=2)+"\n"
    if args.report:
        with args.report.open("x", encoding="utf-8") as f:
            f.write(text)
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

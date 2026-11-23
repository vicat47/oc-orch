#!/usr/bin/env python3
"""OpenSpec fix-attempt-completion-integrity M3：迁移演练测试。

复用 scripts/rehearse_completion_migration.py（内嵌 8 个驱动子进程 + 编排），在
隔离临时目录跑完整演练并断言证据 JSON 中的真实数据与哈希：

- P1 一致备份：WAL 未 checkpoint 写入存在；直接 copy 活动主文件不可用
  （raw_copy_ok=False 且有原因）；backup API 快照含 marker、integrity ok；
- P2 restore 后引用/表/行一致（新旧代码都验证）；
- P3 新代码只读旧数据且行级不变；
- P4 legacy 副本补收集产生新版本，旧 turn/旧 ACK 不回写；
- P5 新协议完整记录（oc-orch.result.v1 / rv1、版本文件、RESULT_READY 通知）；
- P6 混用：新 verify 拒绝（RESULT_CHANGED），新 collect 以新版本修复；
  同一混用状态旧 verify 直接接受（降级不能维持新约束）。

不调用真实服务、不读生产台账（全部台账在临时目录）、不改业务代码。
"""
from __future__ import annotations

from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
SCRIPT = REPO / "scripts" / "rehearse_completion_migration.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("rehearse_completion_migration", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _run_rehearsal(work: Path, evidence: Path):
    mod = _load_script()
    with patch.object(sys, "argv", ["rehearse_completion_migration.py",
                                    "--work-dir", str(work),
                                    "--evidence", str(evidence)]):
        rc = mod.main()
    ev_path = evidence / "rehearse-evidence.json"
    assert ev_path.is_file(), "演练必须产出 rehearse-evidence.json"
    data = json.loads(ev_path.read_text(encoding="utf-8"))
    return rc, data


def _check_rehearsal(tmp_path: Path):
    """完整 M3 演练必须全部通过且关键事实真实。"""
    work = tmp_path / "work"
    evidence = tmp_path / "evidence"
    rc, data = _run_rehearsal(work, evidence)

    assert rc == 0, f"演练 exit 应为 0，实际 {rc}"
    assert data["all_asserts_passed"] is True
    assert data["failures"] == []
    # 每个阶段 rc==0 且有信封
    for phase in data["phases"]:
        assert phase["rc"] == 0, f"{phase['name']} rc={phase['rc']}"
        assert phase["envelope_ok"] is True

    # P1 一致备份：直拷不可用 / backup API 完整
    p1 = data.get("p1") or {}
    assert p1.get("schema_version") == "1"
    assert p1.get("journal_mode") in ("wal", "WAL")
    assert p1.get("wal_exists") is True
    assert p1.get("backup_marker_count") == 1, "backup API 快照必须含未 checkpoint marker"
    assert p1.get("backup_attempt_count") == 2
    assert p1.get("backup_integrity") == "ok"
    assert p1.get("raw_copy_ok") is False, "直接 copy 活动主文件不可作为完整备份"
    assert p1.get("raw_copy_error"), "raw copy 必须有失败原因"

    # P2 restore 引用/表/行一致
    assert (data.get("p2_restored_old") or {}).get("refs_ok") is True
    assert (data.get("p2_restored_old") or {}).get("tables_match") is True
    assert (data.get("p2_restored_new") or {}).get("readonly_rows_unchanged") is True

    # P3 新代码只读旧数据
    assert (data.get("p3_legacy_new") or {}).get("readonly_rows_unchanged") is True
    assert (data.get("p3_legacy_new") or {}).get("readonly_after_reopen_rows_same") is True

    # P4 补收集：版本产生、旧 turn/旧 ACK 不回写
    p4 = data.get("p4_legacy_recovery") or {}
    assert (p4.get("L2_collect") or {}).get("effective") is True
    assert (p4.get("L2_collect") or {}).get("result_version")
    assert (p4.get("L2_version_doc") or {}).get("schema") == "oc-orch.result.v1"
    assert (p4.get("L2_version_doc") or {}).get("protocol_version") == "rv1"
    assert (p4.get("L2_after") or {}).get("turns_unchanged") is True
    assert (p4.get("L2_after") or {}).get("old_acked_unchanged") is True
    assert (p4.get("L2_after") or {}).get("new_notification_pending") is True
    assert (p4.get("L2_after") or {}).get("result_path_is_version") is True

    # P5 新协议完整记录
    p5 = data.get("p5_fresh_new") or {}
    assert (p5.get("collect") or {}).get("effective") is True
    vd = p5.get("version_doc") or {}
    assert vd.get("schema") == "oc-orch.result.v1"
    assert vd.get("protocol_version") == "rv1"
    assert vd.get("result_version") == (p5.get("collect") or {}).get("result_version")
    cv = p5.get("compat_view") or {}
    assert cv.get("exists") is True
    assert cv.get("db_pointer_is_version") is True
    assert cv.get("flat_is_not_authoritative") is True
    notif = p5.get("notification") or {}
    assert notif.get("state") == "pending"
    assert notif.get("payload_ready") is True
    assert notif.get("payload_classification") == "RESULT_READY"

    # P6a 混用拒绝 + 修复
    p6a = data.get("p6_mixa_refuse") or {}
    nv = p6a.get("new_verify") or {}
    assert nv.get("rejected") is True and nv.get("code") == "RESULT_CHANGED"
    assert (p6a.get("new_collect") or {}).get("effective") is True
    assert (p6a.get("healed") or {}).get("result_path_is_version") is True
    na = p6a.get("notifications_after") or {}
    assert na.get("acked_v1_preserved") is True
    assert na.get("pending_v2_created") is True
    assert (p6a.get("new_verify_after_heal") or {}).get("ok") is True

    # P6b 旧 collect 破坏性改写 + 同状态旧 verify 降级接受
    p6b_old = data.get("p6_mixb_mixold") or {}
    assert (p6b_old.get("mix_after") or {}).get("result_path_is_flat") is True
    assert (p6b_old.get("mix_after") or {}).get("version_file_sha_unchanged") is True
    p6b = data.get("p6_mixb_downgrade") or {}
    assert (p6b.get("old_verify") or {}).get("accepted") is True, \
        "同一混用状态旧 verify 直接接受 = 降级不能维持新约束"

    # 证据清单存在且与磁盘实际一致
    manifest = json.loads(
        (evidence / "manifest-sha256.json").read_text(encoding="utf-8"))
    assert manifest, "manifest-sha256.json 不应为空"
    for rel, sha in manifest.items():
        f = evidence / rel
        assert f.is_file(), f"manifest 引用的文件存在: {rel}"
        assert sha == _sha256(f), f"manifest 哈希一致: {rel}"


class MigrationTests(unittest.TestCase):
    def test_m3_rehearsal_all_green(self):
        """默认 unittest 与 pytest 都必须发现完整离线迁移演练。"""
        if not os.environ.get("OC_ORCH_MIGRATION_BASELINE_REPO"):
            probe = subprocess.run(
                ["git", "cat-file", "-e", _load_script().BASE + "^{commit}"],
                cwd=REPO, capture_output=True)
            if probe.returncode:
                self.skipTest("旧版本需通过 OC_ORCH_MIGRATION_BASELINE_REPO 提供")
        with tempfile.TemporaryDirectory(
                prefix="test-completion-migration-",
                dir=os.environ.get("TMPDIR") or "/tmp/opencode") as tmp:
            output = io.StringIO()
            with redirect_stdout(output):
                try:
                    _check_rehearsal(Path(tmp))
                except AssertionError as exc:
                    self.fail(f"{exc}\n{output.getvalue()}")


if __name__ == "__main__":
    unittest.main()

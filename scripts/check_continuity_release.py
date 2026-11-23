#!/usr/bin/env python3
"""仅在显式一致备份副本核验发布写契约/旧写拒绝/保留新事实与本地shadow。

不停止生产、不更换入口、不访问OpenCode或邮箱；work必须是尚不存在的新目录。
真实数据库和逐行摘要保留私有目录，stdout只给计数/布尔值/摘要，不含业务正文。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from oc_orch.adapters.sqlite_db import backup_to, init  # noqa: E402
from oc_orch.adapters.sqlite_ledger import Ledger  # noqa: E402
from oc_orch.adapters.writer_guard import GUARD_KEY, install_writer_guard  # noqa: E402
from oc_orch.composition import make_dependency_wait  # noqa: E402
from oc_orch.runtime.email_executor import OperatorEmailExecutor  # noqa: E402


def ro(path):
    conn = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True,
                           isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn


def digest_query(conn, sql, params=()):
    digest = hashlib.sha256()
    count = 0
    for row in conn.execute(sql, params):
        values = [dict(blob_hex=v.hex()) if isinstance(v, bytes) else v for v in row]
        digest.update(json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode())
        digest.update(b"\n")
        count += 1
    return {"count": count, "sha256": digest.hexdigest()}


def table_facts(conn):
    out = {}
    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name NOT LIKE 'sqlite_%' ORDER BY name"):
        name = row[0]
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{name}")')]
        order = ",".join('"' + c.replace('"', '""') + '"' for c in cols)
        out[name] = digest_query(conn, f'SELECT * FROM "{name}" ORDER BY {order}')
    return out


# 纯副本矩阵轮次上限：0..10 小界限，解析阶段即拒绝越界，避免在创建任何目录
# 或复制数据库之前被负数/巨值刷爆。
MAX_COPY_MATRIX = 10


def _bounded_copy_matrix(value: str) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(
            f"--copy-matrix 必须是 0..{MAX_COPY_MATRIX} 的整数，收到 {value!r}")
    if not (0 <= n <= MAX_COPY_MATRIX):
        raise argparse.ArgumentTypeError(
            f"--copy-matrix 越界：{n}（允许 0..{MAX_COPY_MATRIX}）")
    return n


def _validate_report_path(parser, report: str, backup, work) -> Path:
    """--report 安全性：任一违规直接 parser.error（exit 2），从不 unlink/覆盖。"""
    rp = Path(report).resolve()
    br = Path(backup).resolve()
    w = Path(work).resolve()
    # 更具体的拒绝原因优先（备份等值 > 已存在 > 路径重叠 > 目录形状/可写）
    if rp == br:
        parser.error("--report 不得与 --backup 同一路径")
    if rp.exists():
        parser.error(f"--report 已存在，拒绝覆盖用户内容：{rp}")
    if rp.name == "ledger.db" and w in rp.parents:
        parser.error("--report 不得落在 work 内的 ledger.db 路径（会与副本叠写）")
    if rp.parent.exists() and not rp.parent.is_dir():
        parser.error(f"--report 父路径不是目录，拒绝输出：{rp.parent}")
    parent = rp.parent
    while not parent.exists():
        parent = parent.parent
    if not os.access(str(parent), os.W_OK | os.X_OK):
        parser.error(f"--report 父目录不可写，拒绝输出：{parent}")
    return rp


class NoNetworkMail:
    def __init__(self):
        self.calls = 0

    def send(self, **kwargs):
        self.calls += 1
        raise AssertionError("发布副本禁止邮件网络")

    def search_receipt(self, **kwargs):
        self.calls += 1
        raise AssertionError("发布副本禁止邮箱查询")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--old-source", required=True)
    parser.add_argument("--copy-matrix", type=_bounded_copy_matrix, default=0,
                       metavar="N",
                       help=f"纯副本矩阵：额外执行 N 轮 backup→restore→backup，"
                            f"逐轮校验全表事实摘要逐字一致（0..{MAX_COPY_MATRIX}，默认 0）")
    parser.add_argument("--report", default=None, metavar="JSON",
                        help="把机器可读摘要（含 copy-matrix/覆盖报告）写入该文件；"
                             "缺省只在 stdout 打印")
    args = parser.parse_args()
    backup = Path(args.backup).resolve()
    work = Path(args.work).resolve()
    report_path = _validate_report_path(parser, args.report, backup, work) \
        if args.report else None
    if not backup.is_file() or work.exists():
        parser.error("需要既有备份文件和尚不存在的副本目录")
    os.umask(0o077)
    work.mkdir(parents=True, mode=0o700)
    home = work / "guarded"
    home.mkdir()
    source = ro(backup)
    try:
        before = table_facts(source)
        max_audit = source.execute("SELECT COALESCE(MAX(id),0) FROM audit").fetchone()[0]
        backup_to(source, str(home / "ledger.db"))
    finally:
        source.close()
    conn = init(str(home))
    try:
        installed = install_writer_guard(conn, actor="release-copy-check")
        after = table_facts(conn)
        unchanged = {t: after[t] == fact for t, fact in before.items() if t not in ("meta", "audit")}
        old_audit = digest_query(conn, "SELECT * FROM audit WHERE id<=? ORDER BY id", (max_audit,))
        old_meta = digest_query(conn, "SELECT * FROM meta WHERE key<>? ORDER BY key", (GUARD_KEY,))
        added = conn.execute("SELECT action FROM audit WHERE id>?", (max_audit,)).fetchall()
        assert all(unchanged.values()) and old_audit == before["audit"] and old_meta == before["meta"]
        assert [r[0] for r in added] == ["writer-contract.install"]
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"

        # 使用真实旧实现（不是expect_version桩），只对副本试写一个隔离meta键。
        code = ("from oc_orch.db import init; from oc_orch.ledger import Ledger; "
                "import sys; c=init(sys.argv[1]); "
                "Ledger(c,sys.argv[1]).set_meta('release-old-writer-probe','must-not-exist')")
        env = dict(os.environ, PYTHONPATH=str(Path(args.old_source).resolve() / "src"),
                   PYTHONDONTWRITEBYTECODE="1", OC_ORCH_HOME=str(home))
        old = subprocess.run([sys.executable, "-B", "-c", code, str(home)], cwd=str(work),
                             env=env, capture_output=True, text=True, timeout=45)
        old_refused = old.returncode != 0 and "oc_orch_writer_contract" in old.stderr
        assert old_refused, "真实旧schema3实现未被拒绝"
        assert conn.execute("SELECT 1 FROM meta WHERE key='release-old-writer-probe'").fetchone() is None
        assert table_facts(conn) == after, "旧写失败后仍有数据变化"

        # 保留新事实的恢复是新代码继续读写现账，不倒回历史备份或解开写契约。
        ledger = Ledger(conn, str(home))
        ledger.set_meta("release-copy:unknown-input", "same-original-message-id")
        restore = work / "retained-state"
        restore.mkdir()
        backup_to(conn, str(restore / "ledger.db"))
        restored = init(str(restore))
        try:
            assert table_facts(restored) == table_facts(conn)
            assert Ledger(restored, str(restore)).get_meta("release-copy:unknown-input") == "same-original-message-id"
        finally:
            restored.close()

        # shadow只在副本，纯本地条件和健康投影；绝不启动Supervisor/Client。
        service = make_dependency_wait(ledger)
        rounds = [service.reconcile(limit=500), service.reconcile(limit=500)]
        health = service.business_health(limit=500)
        transport = NoNetworkMail()
        email = OperatorEmailExecutor(str(home), transport=transport).tick()
        assert transport.calls == 0 and email["ok"]
        private = {"before": before, "after_guard_install": after, "rounds": rounds, "health": health}
        (work / "private-details.json").write_text(json.dumps(private, ensure_ascii=False, indent=2) + "\n")
        summary = {
            "ok": True, "guard": installed, "schema_version": "3", "integrity": "ok",
            "business_tables_unchanged": unchanged, "old_audit_unchanged": True,
            "old_meta_unchanged": True, "guard_audit_added": 1,
            "actual_old_schema3_writer_refused": old_refused,
            "refused_old_write_zero_changes": True, "retained_new_facts_restore": True,
            "shadow_rounds": [{k: r.get(k) for k in ("scanned", "ready", "waiting", "needs_human", "rejected")}
                              for r in rounds],
            "business_health_scanned": health["scanned"], "email_default_off_external_calls": transport.calls,
            "no_supervisor_or_opencode_client_started": True,
        }

        # ---- 纯副本矩阵（可选）：backup→restore→backup 逐轮全表事实摘要一致。
        # 每轮只复制/打开/校验，不写业务、不启动 Client/Supervisor；副本目录
        # 全部是 work 下新建的子目录，绝不覆盖/删除已有内容。
        final_facts = table_facts(conn)
        copy_cycles: list[dict] = []
        for i in range(args.copy_matrix):
            copy_dir = work / f"copy-{i}"
            if copy_dir.exists():
                parser.error(f"副本目录 {copy_dir} 已存在；拒绝覆盖/删除内容")
            copy_dir.mkdir()
            backup_to(conn, str(copy_dir / "ledger.db"))
            opened = init(str(copy_dir))
            try:
                opened_facts = table_facts(opened)
                stable = opened_facts == final_facts
                if not stable:
                    raise AssertionError(f"第 {i} 轮副本事实摘要不一致")
                copy_cycles.append({"round": i, "stable": True,
                                    "digest": final_facts})
            finally:
                opened.close()
        summary["copy_matrix"] = {
            "cycles": args.copy_matrix, "all_stable": True,
            "rounds": [{"round": c["round"], "stable": c["stable"]}
                       for c in copy_cycles],
        }
        # ---- 机器可读覆盖报告 -------------------------------------------
        summary["coverage"] = {
            "schema": "oc-orch.r4-release-matrix.v1",
            "guard_contract": GUARD_KEY,
            "old_schema3_connection_refused": True,
            "actual_old_implementation_refused": old_refused,
            "retained_new_facts_preserved": True,
            "pure_copy_cycles_stable": args.copy_matrix > 0,
            "matrix_tests": ("tests/test_continuity_release_matrix.py + "
                             "tests/continuity_matrix_fixtures.py；覆盖表见 "
                             "continuity_matrix_fixtures.COVERAGE_TABLE"),
            "exhaustive_cartesian": False,
            "scope": "发布副本核验（copies only）：guard/旧写拒绝/保留新事实/"
                     "shadow/纯副本轮次；不是 openspec 36 场景或功能全量验收",
        }
        if args.report:
            if report_path.exists():
                parser.error(f"--report 已存在（含运行期被创建），拒绝覆盖：{report_path}")
            ledger_paths = {Path(bp).resolve() for bp in (
                [str(backup), str(home / "ledger.db"), str(restore / "ledger.db")]
                + [str(work / f"copy-{i}" / "ledger.db")
                   for i in range(args.copy_matrix)])}
            if report_path in ledger_paths:
                parser.error("--report 与 ledger.db 路径重叠，拒绝输出")
            try:
                report_path.parent.mkdir(parents=True, exist_ok=True)
                with open(report_path, "x", encoding="utf-8") as fh:
                    fh.write(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
            except FileExistsError:
                parser.error(f"--report 已被既有/并发文件占用，拒绝覆盖：{report_path}")
            except OSError as exc:
                parser.error(f"--report 输出失败（不覆盖用户内容）：{exc}")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    main()

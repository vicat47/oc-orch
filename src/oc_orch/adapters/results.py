"""结果收集与验收检查适配层：文件/命令 I/O；验收业务判定委托 application。

本模块是兼容旧导入路径 oc_orch.results 的权威实现（root results.py 为
sys.modules 别名指向本模块），承担：
- 结果收集：读 session 消息、写结果文件、更新交付物事实（collect_attempt）；
- 检查命令：子进程执行并记录证据（run_checks）；
- 产物证据读取与核验（read_result_artifact）；
- 验收门查询（verification_gate）。

事务外验收准备的业务判定与 verify 编排位于 application.verification；本模块
只提供窄 I/O（read_result/run_checks_io/clock）并注入默认实现，保持旧入口
prepare_verification/verify_attempt 的调用签名与 mock.patch 拦截能力。
"""

from __future__ import annotations

import glob as globmod
import fnmatch
import hashlib
import json
import os
import stat as statmod
import subprocess
import tempfile
import time
from typing import TYPE_CHECKING, Any, Iterable

from ..application import verification as _verify
from ..application import collection as _collection
from ..domain.errors import LedgerError
from ..domain import results as _result_rules
from ..domain.results import BASELINE_SCHEMA, canonical_json
from ..domain.rules import (
    RESULT_ARTIFACT_SCHEMA,
    VERIFIABLE_STATES,
    verification_gate_rows,
)
from ..util import atomic_write, json_dumps, json_loads, now_ms, redact
from .config import load_config

if TYPE_CHECKING:
    from ..application.ports import OpenCodeClient


def _msg_time(m: dict) -> int:
    t = m.get("time") or {}
    return int(t.get("created") or 0)


# ---- 统一交付扫描契约（D5）：file/dir/glob/symlink 完整、有界、稳定 -----
def _budget(budgets: dict | None, key: str, default: int) -> int:
    try:
        v = int((budgets or {}).get(key, default))
        return v if v > 0 else default
    except (TypeError, ValueError):
        return default


def _scan_budget_state(budgets: dict | None, *,
                       target_ref: str = "deliverable_scan") -> dict:
    """共享扫描预算（条目数/字节数），读取过程中逐次累计扣减。

    budgets 可为完整运行配置（含 deliverable_scan_max_entries/max_bytes 键）
    或显式覆盖 dict；None 时用默认值。单一状态对象贯穿一次扫描，
    不允许各分支用 10**9 之类的独立大预算绕过配置。
    """
    return {
        "entries_total": _budget(budgets, "deliverable_scan_max_entries", 10000),
        "bytes_total": _budget(budgets, "deliverable_scan_max_bytes",
                               512 * 1024 * 1024),
        "entries_left": [None],   # 惰性初始化：共享状态首次使用时对齐
        "bytes_left": [None],
        "truncated": False,
    }


def _budget_entries(budget: dict) -> list[int]:
    if budget["entries_left"][0] is None:
        budget["entries_left"][0] = budget["entries_total"]
    return budget["entries_left"]


def _budget_bytes(budget: dict) -> list[int]:
    if budget["bytes_left"][0] is None:
        budget["bytes_left"][0] = budget["bytes_total"]
    return budget["bytes_left"]


def _stat_key(st: os.stat_result) -> tuple:
    """稳定性键：ns 级 mtime/ctime + 秒级值 + size + inode/device + mode。

    秒级 int(mtime) 会让"同秒同长度"的修改逃过不稳定检测；ns 粒度加上
    inode/ctime 等实际 stat 身份，仅用于读取前后一致性观测（不是新鲜度）。
    注：经序列构造的 stat_result 的 *_ns 属性为 None（CPython 行为），
    因此同时保留秒级标量，任一粒度变化都判定不稳定。
    """
    return (st.st_mtime_ns, st.st_mtime, st.st_ctime_ns, st.st_ctime,
            st.st_size, st.st_ino, st.st_dev, st.st_mode)


def _hash_file_strict(path: str, budget: dict) -> tuple[str | None, str | None]:
    """稳定内容摘要：返回 (digest, reason)；reason=None 表示成功。

    - stat 失败（含权限/消失）→ ("stat-failed")；打开/读取失败 → ("read-failed")；
    - 预算按**实际读取字节立即扣减**（读取循环内逐次累计，unstable/失败
      支路消耗的预算同样计入——扫描失败的成本已经发生，不得让持续变化的
      目录反复获得"免费"重试预算）；stat 预检 + 读取中累计，文件读取中
      增长超出剩余预算 → ("scan-budget-exceeded")；
    - 读取前后 stat 键（ns/size/identity/ctime）不一致 → ("scan-unstable")。
    """
    bytes_left = _budget_bytes(budget)
    try:
        st1 = os.stat(path)
    except OSError:
        return None, "stat-failed"
    if not statmod.S_ISREG(st1.st_mode):
        return None, "not-regular-file"
    if st1.st_size > bytes_left[0]:
        budget["truncated"] = True
        return None, "scan-budget-exceeded"
    h = hashlib.sha256()
    consumed = 0
    try:
        with open(path, "rb") as f:
            while True:
                chunk = f.read(min(1024 * 1024, bytes_left[0] + 1))
                if not chunk:
                    break
                consumed += len(chunk)
                bytes_left[0] -= len(chunk)  # 实际读取立即扣减（含失败支路）
                if bytes_left[0] < 0:
                    budget["truncated"] = True
                    return None, "scan-budget-exceeded"
                h.update(chunk)
        st2 = os.stat(path)
    except OSError:
        return None, "read-failed"
    if _stat_key(st2) != _stat_key(st1):
        return None, "scan-unstable"
    return h.hexdigest(), None


class _ManifestScanner:
    """一次有界扫描：目录只枚举一次，glob 未匹配条目也计费；不跟随目录链接。"""

    def __init__(self, budget: dict):
        self.budget = budget
        self.errors: list[str] = []
        self.listings: dict[str, list[str]] = {}
        self.directories: dict[str, tuple] = {}
        self.counted: set[str] = set()

    def reserve(self, path: str) -> bool:
        if path in self.counted:
            return True
        remaining = _budget_entries(self.budget)
        if remaining[0] <= 0:
            self.budget["truncated"] = True
            self.errors.append("entry-budget-exceeded")
            return False
        remaining[0] -= 1
        self.counted.add(path)
        return True

    def lstat(self, path: str, *, missing_ok=False):
        try:
            return os.lstat(path)
        except OSError as exc:
            if not (missing_ok and isinstance(exc, FileNotFoundError)):
                self.errors.append(f"lstat-failed:{path}:{type(exc).__name__}")
            return None

    def list_dir(self, path: str) -> list[str]:
        if path in self.listings:
            return self.listings[path]
        names: list[str] = []
        self.listings[path] = names
        st = self.lstat(path)
        if st is None:
            return names
        if not statmod.S_ISDIR(st.st_mode):
            self.errors.append(f"directory-not-traversable:{path}")
            return names
        self.directories[path] = _stat_key(st)
        try:
            with os.scandir(path) as entries:
                for entry in entries:
                    if not self.reserve(os.path.join(path, entry.name)):
                        break  # 只多读一个条目以判超限，不先物化整个目录。
                    names.append(entry.name)
        except OSError as exc:
            self.errors.append(f"dir-list-failed:{path}:{type(exc).__name__}")
        names.sort()
        return names

    def glob_paths(self, pattern: str) -> list[str]:
        if not globmod.has_magic(pattern):
            return [pattern] if self.lstat(pattern, missing_ok=True) else []
        parts = pattern.split(os.sep)
        first = next(i for i, part in enumerate(parts) if globmod.has_magic(part))
        base = os.sep.join(parts[:first]) or (os.sep if first else ".")
        st = self.lstat(base, missing_ok=True)
        if st is None:
            return []
        if not statmod.S_ISDIR(st.st_mode):
            self.errors.append(f"directory-not-traversable:{base}")
            return []
        patterns = parts[first:]
        pending = [(base, 0)]
        visited = set()
        matched = set()
        while pending:
            directory, index = pending.pop()
            if (directory, index) in visited:
                continue
            visited.add((directory, index))
            head = patterns[index]
            last = index == len(patterns) - 1
            if head == "**":
                if last:
                    matched.add(directory)
                else:
                    pending.append((directory, index + 1))  # 零层匹配
            for name in self.list_dir(directory):
                if name.startswith(".") and not head.startswith("."):
                    continue
                child = os.path.join(directory, name)
                if head == "**":
                    if last:
                        matched.add(child)
                    st = self.lstat(child)
                    if st is not None and statmod.S_ISDIR(st.st_mode):
                        pending.append((child, index))
                elif fnmatch.fnmatchcase(name, head):
                    if last:
                        matched.add(child)
                    else:
                        st = self.lstat(child)
                        if st is not None and statmod.S_ISDIR(st.st_mode):
                            pending.append((child, index + 1))
        return sorted(matched)

    def entry(self, path: str) -> dict:
        st = self.lstat(path)
        item = {"path": path, "type": "unknown"}
        if st is None:
            return item
        if statmod.S_ISLNK(st.st_mode):
            try:
                item.update(type="link", link=os.readlink(path))
                if _stat_key(os.lstat(path)) != _stat_key(st):
                    self.errors.append(f"scan-link-unstable:{path}")
            except OSError:
                self.errors.append(f"readlink-failed:{path}")
            return item
        if statmod.S_ISDIR(st.st_mode):
            return {"path": path, "type": "dir"}
        if not statmod.S_ISREG(st.st_mode):
            self.errors.append(f"unsupported-file-type:{path}")
            return {"path": path, "type": "other"}
        digest, reason = _hash_file_strict(path, self.budget)
        item.update(type="file", sha256=digest)
        if reason:
            item["reason"] = reason
            self.errors.append(f"{reason}:{path}")
        return item

    def recheck(self):
        # 还比较名字集合：短时间内元数据时钟可能相同，不能只依赖时间戳。
        # 每目录复查最多原条目数+1；不完整扫描已拒绝，无须继续枚举。
        if self.errors or self.budget["truncated"]:
            return
        for path, before in self.directories.items():
            expected = self.listings[path]
            names = []
            try:
                with os.scandir(path) as entries:
                    for entry in entries:
                        names.append(entry.name)
                        if len(names) > len(expected):
                            break
            except OSError as exc:
                self.errors.append(f"dir-list-failed:{path}:{type(exc).__name__}")
            after = self.lstat(path)
            if (after is None or _stat_key(after) != before
                    or sorted(names) != expected):
                self.errors.append(f"scan-dir-unstable:{path}")


def _scan_artifact_manifest(target: str, kind: str,
                            budget: dict) -> tuple[list[dict], bool, list[str]]:
    scanner = _ManifestScanner(budget)
    manifest = []
    if kind == "dir":
        pending = [target]
        while pending:
            directory = pending.pop()
            for name in scanner.list_dir(directory):
                item = scanner.entry(os.path.join(directory, name))
                manifest.append(item)
                if item["type"] == "dir":
                    pending.append(item["path"])
    else:
        for path in scanner.glob_paths(target):
            if scanner.reserve(path):
                manifest.append(scanner.entry(path))
    scanner.recheck()
    manifest.sort(key=lambda item: item["path"])
    return manifest, not scanner.errors and not budget["truncated"], scanner.errors


def _scan_file_snapshot(target: str, budget: dict) -> dict:
    """单文件声明的同一预算/稳定性契约；权限错误不能由 exists() 吞成 missing。"""
    remaining = _budget_entries(budget)
    if remaining[0] <= 0:
        budget["truncated"] = True
        return {"status": "unknown", "reason": "entry-budget-exceeded"}
    remaining[0] -= 1
    try:
        before = os.lstat(target)
    except FileNotFoundError:
        return {"status": "missing"}
    except OSError:
        return {"status": "unknown", "reason": "stat-failed"}
    try:
        linked = statmod.S_ISLNK(before.st_mode)
        resolved = os.path.realpath(target) if linked else target
        digest, reason = _hash_file_strict(target, budget)
        if reason:
            return {"status": "unknown", "reason": reason}
        st = os.stat(target)
        if _stat_key(os.lstat(target)) != _stat_key(before):
            return {"status": "unknown", "reason": "scan-unstable"}
        item = {"status": "present", "sha256": digest,
                "size": st.st_size, "mtime_ns": st.st_mtime_ns}
        if linked:
            item.update(link=True, resolved=resolved)
        return item
    except OSError:
        return {"status": "unknown", "reason": "stat-failed"}


def _manifest_digest(manifest: list[dict]) -> str:
    return _result_rules._sha256_text(canonical_json(manifest))


def _baseline_digest(baseline: dict) -> str:
    return _result_rules.sha256_text(canonical_json({
        "items": baseline["items"], "coverage": baseline["coverage"],
        "attempt_id": baseline["attempt_id"],
    }))


def scan_deliverable_baseline(ledger, attempt_id: str,
                              *, budgets: dict | None = None,
                              captured_at: int | None = None) -> dict:
    """派单前封存交付基线：results/<attempt>/baseline.json，一次封存不可覆写。

    统一扫描契约（D5）：file/dir/glob/symlink 完整、有界（预算读取中累计）、
    稳定（ns 级 stat 键）；任何条目级 IO 失败/不稳定/预算超限都使条目为
    unknown 且 coverage 不完整——抽样或部分清单不能宣称完整基线。
    """
    att = ledger.get_attempt(attempt_id)
    sealed = load_baseline(ledger, attempt_id)
    if sealed is not None:
        return sealed
    dels = ledger.deliverables(attempt_id)
    budget = _scan_budget_state(budgets)
    items: list[dict] = []
    coverage_truncated = False
    coverage_errors: list[str] = []
    for d in dels:
        target = d["target"]
        if not os.path.isabs(target):
            target = os.path.join(att["directory"], target)
        target = os.path.expanduser(target)
        item: dict[str, Any] = {"target": target, "kind": d["kind"],
                                "declared": d["target"]}
        if d["kind"] == "file":
            item.update(_scan_file_snapshot(target, budget))
            if item["status"] == "unknown":
                coverage_errors.append(f"{item['reason']}:{d['target']}")
            coverage_truncated |= budget["truncated"]
        elif d["kind"] in ("dir", "glob"):
            if d["kind"] == "dir":
                try:
                    os.lstat(target)
                except FileNotFoundError:
                    item.update({"status": "missing", "manifest_missing": True})
                    items.append(item)
                    continue
                except OSError:
                    pass  # 扫描器会记录未知原因，不能当作不存在。
            manifest, complete, reasons = _scan_artifact_manifest(
                target, d["kind"], budget)
            item.update({
                "status": ("missing" if d["kind"] == "glob" and not manifest
                           else "present") if complete else "unknown",
                "manifest": manifest,
                "manifest_sha256": _manifest_digest(manifest) if complete
                else None,
                "manifest_missing": d["kind"] == "glob" and not manifest and complete,
                "entry_count": len(manifest),
            })
            if not complete:
                item["reason"] = "scan-incomplete"
                item["reasons"] = reasons[:50]
                coverage_errors.extend(
                    f"scan-incomplete:{r}" for r in reasons[:5])
                coverage_truncated = True
        else:  # command/note 不在派单前执行
            item["status"] = "not-scanned"
        items.append(item)
    baseline = {
        "schema": BASELINE_SCHEMA,
        "attempt_id": attempt_id,
        "captured_at": captured_at or now_ms(),
        "coverage": {
            "complete": not coverage_truncated and not coverage_errors,
            "entries": (budget["entries_total"]
                        - (_budget_entries(budget)[0] or 0)),
            "bytes_budget_left": _budget_bytes(budget)[0],
            "truncated": coverage_truncated,
            "errors": coverage_errors[:50],
        },
        "items": items,
    }
    results_dir = os.path.join(ledger.home, "results", attempt_id)
    os.makedirs(results_dir, mode=0o700, exist_ok=True)
    path = os.path.join(results_dir, "baseline.json")
    baseline["baseline_sha256"] = _baseline_digest(baseline)
    fd, temporary = tempfile.mkstemp(prefix=".baseline-", dir=results_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(baseline, ensure_ascii=False, indent=1))
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            sealed = load_baseline(ledger, attempt_id)
            if sealed is None:
                raise LedgerError("BASELINE_INVALID", "并发封存的基线已经消失")
            return sealed
    finally:
        os.unlink(temporary)
    baseline["path"] = path
    return baseline


def load_baseline(ledger, attempt_id: str) -> dict | None:
    """读取不可变基线并复核封存摘要；缺失/损坏不能降级成 legacy 或重新采样。"""
    path = os.path.join(ledger.home, "results", attempt_id, "baseline.json")
    execution = ledger.get_attempt(attempt_id)["metadata"].get("execution") or {}
    pinned = execution.get("baseline") or {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError as exc:
        if pinned:
            raise LedgerError("BASELINE_INVALID", "已封存的交付基线缺失") from exc
        return None
    except (OSError, ValueError) as exc:
        raise LedgerError("BASELINE_INVALID", "交付基线不可读或损坏") from exc
    if (not isinstance(data, dict) or data.get("schema") != BASELINE_SCHEMA
            or data.get("attempt_id") != attempt_id
            or not isinstance(data.get("coverage"), dict)
            or not isinstance(data.get("items"), list)
            or any(not isinstance(item, dict) for item in data["items"])):
        raise LedgerError("BASELINE_INVALID", "交付基线身份或结构不符")
    digest = _baseline_digest(data)
    for expected in (data.get("baseline_sha256"), pinned.get("baseline_sha256")):
        if expected is not None and expected != digest:
            raise LedgerError("BASELINE_INVALID", "交付基线与封存摘要不一致")
    return {**data, "path": path, "baseline_sha256": digest}


def _legacy_items_from_evidence(dels: list[dict]) -> dict[str, dict]:
    """无封存基线时，用已记录的交付证据作 legacy-observation 观察基线（M2/T29）。"""
    items: dict[str, dict] = {}
    for d in dels:
        evidence = json_loads(d.get("evidence"), None) if isinstance(
            d.get("evidence"), str) else (d.get("evidence") or None)
        item: dict[str, Any] = {"kind": d["kind"],
                                "status": d.get("status") or "unknown",
                                "legacy_observation": True}
        if isinstance(evidence, dict):
            if evidence.get("sha256") and isinstance(evidence["sha256"], str):
                item["sha256"] = evidence["sha256"]
            elif isinstance(evidence.get("sha256"), dict):
                # 旧 glob/dir 抽样摘要：不完整抽样不能当完整清单证明
                item["sampled"] = True
            if evidence.get("matches") is not None:
                item["sampled"] = True
        items[d["target"]] = item
    return items


def current_deliverable_states(ledger, attempt_id: str) -> list[dict]:
    """当前物理状态扫描（候选值）：persist=False 不写交付行。

    F6：收集/验收准备阶段的扫描只返回候选（不逐项独立事务写交付证据），
    正式交付行由 commit_collection 在收集短事务内原子提交。不做任何
    "persist=True 回退"：底层扫描真实抛出的 TypeError 必须原样暴露，
    不得静默转成持久写；B 的 Ledger.check_deliverables 已支持 persist。
    """
    return ledger.check_deliverables(attempt_id, persist=False)


def compare_provenance(ledger, attempt_id: str, *, baseline: dict | None,
                       current: list[dict],
                       carry: list[dict] | None = None,
                       legacy_rows: list[dict] | None = None,
                       prior_result_items: list[dict] | None = None,
                       budgets: dict | None = None) -> list[dict]:
    """对每个交付目标计算来源维度（new/changed/unchanged/carried/unknown）。

    统一扫描契约（D5）：当前内容摘要与基线同一套完整/有界/稳定扫描——
    file 走 ns 级稳定哈希、dir/glob 走完整清单摘要（链接身份、walk onerror
    记录、枚举中累计预算）；IO 失败/不稳定/预算超限 → unknown，不冒充可比较。
    budgets：运行配置（或显式覆盖），不提供时用默认预算。
    legacy_rows：无封存基线时的既有交付行（必须在本次物理扫描之前取得，
    防止把当前扫描结果自我证明为历史产物）。
    prior_result_items：已提交结果版本的 provenance 摘要（含 sha256）；
    首次正式收集后作为 legacy-observation 观察基线（M2/T27/T28）。
    """
    carry_map = {c["target"]: c for c in (carry or [])}
    base_items: dict[str, dict] = {}
    attempt_has_baseline = baseline is not None
    if baseline:
        for it in baseline.get("items") or []:
            base_items[it.get("declared")] = it
    legacy = False
    if not baseline:
        prior_map = {p.get("target"): p for p in (prior_result_items or [])
                     if p.get("target")}
        legacy_items = _legacy_items_from_evidence(
            legacy_rows if legacy_rows is not None
            else ledger.deliverables(attempt_id))
        merged: dict[str, dict] = {}
        for target, item in legacy_items.items():
            prior = prior_map.get(target)
            if prior and prior.get("target") and (
                    prior.get("sha256") or prior.get("manifest_sha256")):
                # 已提交版本的观察基线优先于行内可变 evidence（sha 可比对）
                item = dict(item, status="present",
                            sha256=prior.get("sha256"),
                            manifest_sha256=prior.get("manifest_sha256"),
                            legacy_observation=True)
            merged[target] = item
        if merged:
            base_items = merged
            legacy = True
    budget = _scan_budget_state(budgets)
    out: list[dict] = []
    for d in current:
        target = d["target"]
        if not os.path.isabs(target):
            target = os.path.join(ledger.get_attempt(attempt_id)["directory"], target)
        target = os.path.expanduser(target)
        cur: dict[str, Any] = {"present": d["status"] in ("present", "verified"),
                               "kind": d["kind"], "status": d["status"]}
        if d["kind"] == "file":
            observed = _scan_file_snapshot(target, budget)
            cur.update(observed)
            cur["present"] = (None if observed["status"] == "unknown"
                              else observed["status"] == "present")
            if observed.get("reason"):
                cur["scan_reason"] = observed["reason"]
        elif d["kind"] in ("dir", "glob"):
            iterator_missing = False
            if d["kind"] == "dir":
                try:
                    os.lstat(target)
                except FileNotFoundError:
                    iterator_missing = True
                except OSError:
                    pass
            if iterator_missing:
                cur["manifest_sha256"] = None
                cur.update(present=False, status="missing")
            else:
                manifest, complete, reasons = _scan_artifact_manifest(
                    target, d["kind"], budget)
                cur["manifest_sha256"] = _manifest_digest(manifest) if complete \
                    else None
                cur["present"] = bool(manifest) if d["kind"] == "glob" else True
                cur["status"] = "present" if cur["present"] else "missing"
                if not complete:
                    cur["present"] = None  # unknown：不完整扫描不可比较
                    cur["status"] = "unknown"
                    cur["scan_reason"] = "scan-incomplete"
                    cur["scan_reasons"] = reasons[:20]
                cur["entry_count"] = len(manifest)
        elif d["kind"] == "command":
            cur["present"] = d["status"] in ("verified",) or d["status"] == "present"
        base_item = base_items.get(d["target"])
        if base_item is None and attempt_has_baseline:
            # 基线存在但未覆盖该目标（本轮新增/后来声明）
            cur["declared_new_target"] = True
        verdict = _result_rules.provenance_for(base_item, cur,
                                               carry_map.get(d["target"]))
        entry = {**d, "status": cur["status"], "present": bool(cur["present"]),
                 "sha256": cur.get("sha256"),
                 "manifest_sha256": cur.get("manifest_sha256"),
                 "provenance": verdict["provenance"],
                 "provenance_detail": verdict["detail"]}
        if cur.get("scan_reason"):
            entry["scan_reason"] = cur["scan_reason"]
        if legacy and base_item is not None:
            entry["legacy_observation"] = True
        out.append(entry)
    return out


# ---- 不可变版本快照（D8；任务 5.1） --------------------------------
# 快照中不参与业务身份比较的易变字段：collected_at 可在同指纹并发复用；
# auto 不入指纹。text/text_chars/text_truncated/stored_text_sha256 由
# result_store_limit 决定（store_limit 变化不改变业务身份）：截断正文与
# 其绑定摘要都随 limit 派生，业务等价只要求各自"自洽"（见
# _snapshot_business_view 中的绑定校验），不要求跨快照同值。
_SNAPSHOT_VOLATILE_KEYS = frozenset(
    {"collected_at", "auto", "text", "text_chars", "text_truncated",
     "stored_text_sha256"})


def _snapshot_business_view(result: dict) -> dict | None:
    """快照业务内容视图（剔除易变字段）；结构异常返回 None。

    附加存储正文完整性校验（正文单变量篡改无法再冒充同业务版本）：
    - text_truncated 为假时，存储 text 的 sha256 必须等于 text_full_sha256
      且 text_chars == len(text)；
    - 截断时用 stored_text_sha256 复核已存正文；text_chars 仍表示完整长度。
    """
    if not isinstance(result, dict) or not isinstance(
            result.get("result_version"), str):
        return None
    view = {k: v for k, v in result.items() if k not in _SNAPSHOT_VOLATILE_KEYS}
    recovery = view.get("recovery_execution")
    if isinstance(recovery, dict):
        # legacy 恢复每次仍重新采证；观察时刻不是业务身份（D8）。只剔除
        # 该易变字段，真实区间、输入/idle 身份与覆盖证据仍参与严格比较。
        view["recovery_execution"] = {
            k: v for k, v in recovery.items() if k != "observed_at"}
    full_sha = result.get("text_full_sha256")
    text = result.get("text")
    truncated = result.get("text_truncated")
    text_chars = result.get("text_chars")
    # B 的 result 构造语义：text_chars 是**完整正文长度**（截断存储时
    # != len(text)），与门面 collect_attempt 的 text_chars 语义一致；
    # 截断存储的长度由绑定摘要与 stored_text_sha256 自证。非截断时
    # 长度必须自洽。
    if not (isinstance(text, str) and isinstance(full_sha, str)
            and len(full_sha) == 64
            and all(c in "0123456789abcdef" for c in full_sha)
            and isinstance(text_chars, int) and text_chars >= len(text)):
        return None
    if truncated:
        # 截断内容必须声明完整正文身份（text_full_sha256）与截断点的绑定
        # 摘要（stored_text_sha256 == sha256(存储 text)）：读取校验重算比对，
        # 换前缀/改截断点即刻失配；业务等价比较不依赖派生长度/摘要值本身。
        bound = result.get("stored_text_sha256")
        if not isinstance(bound, str) or bound != _result_rules.sha256_text(text):
            return None
    else:
        stored = _result_rules.sha256_text(text)
        if stored != full_sha or text_chars != len(text):
            return None  # 未截断却与完整摘要/长度不符：正文被改动
    return view


def _decode_snapshot(raw: bytes) -> dict | None:
    """解码快照字节为 dict；结构/编码异常返回 None（调用方按明确错误拒绝）。"""
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _read_version_file(path: str) -> tuple[bytes | None, dict | None]:
    """读取版本快照文件：返回 (raw, decoded)；OSError → (None, None)。"""
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except OSError:
        return None, None
    return raw, _decode_snapshot(raw)


def _verify_text_digest(text: str) -> str:
    """存储正文摘要（与 domain.sha256_text 相同口径）。"""
    return _result_rules.sha256_text(text)


def publish_version_snapshot(ledger, attempt_id: str, result: dict) -> tuple[str, int, bool]:
    """原子 create-if-absent 发布不可变版本文件（os.link 原子争用创建）。

    返回 (path, collected_at, existed)：已存在时复用原快照的 collected_at，
    保证并发同指纹的两个收集器向台账提交同一时间。

    已存在快照的校验（D8"已存在且校验有效"）：除易变字段（collected_at/
    auto/store_limit 派生的截断正文）外，实际业务内容（交付摘要、消息身份、
    执行边界、指纹、门状态等）必须与候选等价；仅版本/指纹字符串相等但
    正文或交付内容被改动/损坏的快照拒绝复用（RESULT_VERSION_MISMATCH），
    防止 DB 指向内容失真的"同名"版本。
    """
    version = result["result_version"]
    import tempfile as _tempfile
    versions_dir = os.path.join(ledger.home, "results", attempt_id, "versions")
    os.makedirs(versions_dir, mode=0o700, exist_ok=True)
    path = os.path.join(versions_dir, f"{version}.json")
    # 截断正文的绑定摘要：生成方（候选）若未显式提供则在此补齐——
    # stored_text_sha256 == sha256(存储 text)，读取校验时重算比对，
    # 换前缀/改截断点即刻失配。写入前补齐保证新快照总是带绑定。
    if result.get("text_truncated") and "stored_text_sha256" not in result:
        text = result.get("text")
        if isinstance(text, str):
            result["stored_text_sha256"] = _result_rules.sha256_text(text)
    candidate_view = _snapshot_business_view(result)

    def _load_existing() -> tuple[str, int, bool]:
        raw, existing = _read_version_file(path)
        if existing is None:
            raise LedgerError(
                "RESULT_VERSION_UNREADABLE",
                f"版本快照存在但不可读或结构损坏：{path}")
        if (candidate_view is None
                or _snapshot_business_view(existing) != candidate_view):
            raise LedgerError(
                "RESULT_VERSION_MISMATCH",
                f"版本快照存在但实际业务内容/存储正文与候选不一致：{path}",
                {"stored": existing.get("result_version"),
                 "candidate": result.get("result_version")})
        return path, int(existing.get("collected_at") or 0), True

    if os.path.exists(path):
        try:
            return _load_existing()
        except (OSError, ValueError, TypeError):
            raise LedgerError("RESULT_VERSION_UNREADABLE",
                              f"版本快照存在但不可读：{path}") from None
    fd, tmp = _tempfile.mkstemp(prefix=".tmp-", dir=versions_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, 0o600)
        try:
            os.link(tmp, path)  # 原子 create-if-absent
        except FileExistsError:
            return _load_existing()
        except OSError as e:
            raise LedgerError("RESULT_VERSION_WRITE_FAILED",
                              f"写入版本快照失败：{e}") from e
        return path, int(result["collected_at"]), False
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


def update_compat_view(ledger, attempt_id: str, result: dict) -> str:
    """兼容最新视图 results/<attempt>.json：可修复缓存；DB 指针才是权威。

    落后写保护（D8，含真实交错）：写前读指针防止回退已知的新版本；写后
    再复核一次当前指针——A 读 V1 → B 提交 V2（视图/指针已更新）→ A 才写
    V1 的窗口中，A 的写后复核发现 DB 已指向 V2，按 V2 权威快照重建视图。
    权威版本文件不可读时保留刚写入内容（缓存可修复），不抛错、不回退指针。
    """
    path = os.path.join(ledger.home, "results", f"{attempt_id}.json")
    mine = result.get("result_version")
    att = ledger.get_attempt(attempt_id)
    stored = ((att["metadata"] or {}).get("result") or {}).get("version")
    if isinstance(stored, str) and stored and stored != mine:
        # 写前已知更晚版本：直接按权威快照重建，不用旧 result 回写
        rebuilt = _read_version_snapshot(ledger, attempt_id, stored)
        if rebuilt is not None:
            atomic_write(path, json.dumps(rebuilt, ensure_ascii=False,
                                          indent=1), 0o600)
            return path
        return path  # 权威文件暂不可读：保持现状，不写旧内容
    atomic_write(path, json.dumps(result, ensure_ascii=False, indent=1), 0o600)
    # 写后复核：提交与视图写入之间存在并发窗口，以当前 DB 指针修复
    att_after = ledger.get_attempt(attempt_id)
    stored_after = ((att_after["metadata"] or {}).get("result") or {}).get(
        "version")
    if (isinstance(stored_after, str) and stored_after
            and stored_after != mine):
        rebuilt = _read_version_snapshot(ledger, attempt_id, stored_after)
        if rebuilt is not None:
            atomic_write(path, json.dumps(rebuilt, ensure_ascii=False,
                                          indent=1), 0o600)
    return path


def _read_version_snapshot(ledger, attempt_id: str, version: str) -> dict | None:
    """读取不可变权威版本快照；不存在/损坏返回 None（调用方保持缓存现状）。"""
    path = os.path.join(ledger.home, "results", attempt_id, "versions",
                        f"{version}.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _safe_msg_time(m: dict) -> int | None:
    """防御消息时间：未知/非法 created 返回 None，绝不默认为 0。"""
    t = m.get("time")
    if not isinstance(t, dict):
        return None
    v = t.get("created")
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def read_message_window(client, session_id: str, *, max_pages: int,
                        page_size: int, anchor_id: str | None = None,
                        since_ms: int | None = None,
                        until_ms: int | None = None) -> tuple[list[dict], dict]:
    """有界倒序分页窗口读取（F3/T26）：从最近页向前翻至本轮锚点/起界。

    与整段升序翻页不同，复用 session 的远古历史（总量 > max_pages×page_size）
    不会耗尽预算：命中锚点消息（anchor_id）、越过起界（since_ms）或历史耗尽
    （next=None）即停止，只读本轮窗口需要的事实，不把首次 N 条当作完整区间。

    消息最小必要字段：非空字符串 id + 可解析的数字 created。缺 id / created
    未知或非法（含 time 结构损坏）→ page-malformed-message-shape，complete=False；
    不把未知 created 当 0 参与起界/上界判定。锚点消息 created 未知时不能作为
    下界（anchor-bound 不可建立）：有 since_ms 则按 since 降级（anchor_found
    仍如实报告），否则 anchor-created-unknown 判 incomplete。

    覆盖判定（严格，不可静默降级为完整）：
    - 页缺 data/cursor 字段、data 非列表、cursor 非对象 → page-malformed-*；
      适配器协议异常（Client.message_page 抛错，含坏 cursor 4xx）转 coverage.error；
    - 空页带 next 游标 → empty-page-with-next-cursor；重复 next 游标 →
      cursor-loop；预算耗尽未建立下界 → page-budget-exhausted；
    - anchor_id 指定而 since_ms 未给、历史耗尽仍未命中锚点 → anchor-not-found。
    下界口径（coverage["bound"]）：anchor（锚点消息实际时刻）> since（起界，
    跨过或耗尽均视为覆盖了 [since, 最新] 可见区间）> history（无起界的完整
    历史耗尽）。

    返回 (messages, coverage)：messages 为窗口内消息（[下界, until_ms]，
    按 time.created 升序，重复 ID 去重）；coverage 记录 pages/
    duplicates_skipped/anchor_found/bound/since/until/cursor_end/reason/error。
    """
    for name, value in (("since_ms", since_ms), ("until_ms", until_ms)):
        if value is not None and (isinstance(value, bool)
                                  or not isinstance(value, int)):
            return [], {"complete": False, "pages": 0,
                        "duplicates_skipped": 0, "anchor_found": False,
                        "anchor_id": anchor_id, "bound": None,
                        "since": None, "until": until_ms, "cursor_end": None,
                        "reason": f"invalid-bound:{name}", "error": None}
    coverage: dict[str, Any] = {
        "complete": False, "pages": 0, "duplicates_skipped": 0,
        "anchor_found": False, "anchor_id": anchor_id, "bound": None,
        "since": None, "until": until_ms, "cursor_end": None,
        "reason": None,
    }
    collected: list[dict] = []
    seen_ids: set[str] = set()
    seen_cursors: set[str] = set()
    cursor: str | None = None
    reason: str | None = None
    stopped = False
    pages = 0
    try:
        while pages < max_pages:
            page = client.message_page(session_id, limit=page_size,
                                       order="desc", cursor=cursor)
            if not isinstance(page, dict) or "data" not in page \
                    or "cursor" not in page:
                reason = "page-malformed-missing-fields"
                break
            data = page["data"]
            cursors = page["cursor"]
            if not isinstance(data, list) or not isinstance(cursors, dict):
                reason = "page-malformed-shape"
                break
            pages += 1
            coverage["pages"] = pages
            nxt = cursors.get("next")
            if not data:
                if nxt:
                    # 空页声称还有下一页：协议异常，不能当作历史耗尽
                    reason = "empty-page-with-next-cursor"
                    break
                stopped = True  # 历史耗尽
                break
            oldest_time: int | None = None
            for m in data:
                if not isinstance(m, dict):
                    reason = "page-malformed-message-shape"
                    break
                mid = m.get("id")
                if not isinstance(mid, str) or not mid:
                    reason = "page-malformed-message-shape"
                    break
                mtime = _safe_msg_time(m)
                if mtime is None:
                    # 未知 created 不能当 0 参与边界判定：整页不可信
                    reason = "page-malformed-message-shape"
                    break
                if mid in seen_ids:
                    coverage["duplicates_skipped"] += 1
                    oldest_time = mtime
                    continue
                seen_ids.add(mid)
                collected.append(m)
                oldest_time = mtime
                if anchor_id and mid == anchor_id:
                    coverage["anchor_found"] = True
            if reason is not None:
                break
            if anchor_id and coverage["anchor_found"]:
                stopped = True
                break
            if since_ms is not None and (
                    oldest_time is None or oldest_time < since_ms):
                stopped = True  # 已越过起界：本轮窗口下界已建立
                break
            if not nxt:
                stopped = True  # 历史耗尽
                break
            if nxt in seen_cursors:
                reason = "cursor-loop"
                break
            seen_cursors.add(nxt)
            cursor = nxt
        if not stopped and reason is None:
            reason = "page-budget-exhausted"
    except Exception as e:  # 适配器协议异常（D：message_page 抛错）→ 覆盖不足
        coverage["error"] = repr(e)[:200]
        reason = "message-page-error"

    anchor_time = None
    anchor_time_unknown = False
    if anchor_id:
        for m in collected:
            if m.get("id") == anchor_id:
                t = _safe_msg_time(m)
                if t is None:
                    anchor_time_unknown = True
                else:
                    anchor_time = t
                break
    bound = None
    if anchor_time is not None:
        bound = "anchor"
    elif since_ms is not None and stopped and reason is None:
        bound = "since"  # 跨过起界或耗尽（可见历史均 >= since）
    elif anchor_id is None and since_ms is None and stopped and reason is None:
        bound = "history"
    if reason is None:
        if bound is None:
            if anchor_time_unknown and since_ms is not None:
                bound = "since"  # 锚点时间未知：按起界降级，如实报告
            else:
                reason = ("anchor-created-unknown" if anchor_time_unknown
                          else "anchor-not-found"
                          if (anchor_id and since_ms is None)
                          else "page-budget-exhausted")
        else:
            coverage["complete"] = True
    coverage["reason"] = reason
    coverage["bound"] = bound
    coverage["cursor_end"] = cursor
    lower = anchor_time if anchor_time is not None else since_ms
    coverage["since"] = lower
    window = [m for m in collected
              if (lower is None or _safe_msg_time(m) >= lower)
              and (until_ms is None or _safe_msg_time(m) <= until_ms)]
    window.sort(key=lambda m: _safe_msg_time(m) or 0)
    return window, coverage

def extract_assistant_text(messages: list[dict], since_ms: int) -> tuple[str, list[dict], int]:
    texts: list[str] = []
    tools: list[dict] = []
    count = 0
    for m in sorted(messages, key=_msg_time):
        if m.get("type") != "assistant":
            continue
        if _msg_time(m) < since_ms:
            continue
        count += 1
        for c in m.get("content") or []:
            ctype = c.get("type")
            if ctype == "text" and c.get("text"):
                texts.append(str(c["text"]))
            elif ctype == "tool":
                state = c.get("state") or {}
                tools.append({
                    "name": c.get("name"),
                    "status": state.get("status"),
                    "executed": c.get("executed"),
                    "time": c.get("time"),
                })
    return "\n\n".join(texts).strip(), tools, count


def write_partial_snapshot(ledger, attempt_id: str, data: dict) -> str:
    """partial/诊断快照：独立目录，不覆盖正式指针或版本。"""
    diag_dir = os.path.join(ledger.home, "results", attempt_id, "partial")
    os.makedirs(diag_dir, mode=0o700, exist_ok=True)
    path = os.path.join(diag_dir, f"partial-{data.get('collected_at')}.json")
    atomic_write(path, json.dumps(data, ensure_ascii=False, indent=1), 0o600)
    return path


def read_anchor(client, session_id: str, message_id: str) -> dict | None:
    """定点读取输入锚点消息；适配器无该能力时返回 None（由调用方降级）。"""
    getter = getattr(client, "get_message", None)
    if getter is None:
        return None
    try:
        return getter(session_id, message_id)
    except Exception:
        return None


def read_legacy_result_snapshot(path: str) -> dict | None:
    """读取桥接所需旧快照；语义对应性由 collection 校验，不补造历史证据。"""
    _raw, data = _read_version_file(os.path.expanduser(path))
    return data


class _CollectionIO:
    """application.collection 的窄 I/O 绑定（本模块函数单点实现）。

    read_message_window（F3）：B 的正式收集用例经此读取本轮消息窗口，
    签名/覆盖语义见该函数 docstring；锚点/起界由应用层传入。
    """

    extract_assistant_text = staticmethod(extract_assistant_text)
    load_baseline = staticmethod(load_baseline)
    compare_provenance = staticmethod(compare_provenance)
    current_deliverable_states = staticmethod(current_deliverable_states)
    publish_version_snapshot = staticmethod(publish_version_snapshot)
    update_compat_view = staticmethod(update_compat_view)
    write_partial_snapshot = staticmethod(write_partial_snapshot)
    read_anchor = staticmethod(read_anchor)
    read_message_window = staticmethod(read_message_window)
    read_legacy_result_snapshot = staticmethod(read_legacy_result_snapshot)


def collect_attempt(ledger, client: "OpenCodeClient", attempt_id: str, cfg: dict,
                    *, auto: bool = False, partial: bool = False,
                    carry_manifest: dict | None = None,
                    clock=None) -> dict:
    """结果收集兼容门面：编排统一在 application.collection（D7）。

    保留旧签名 (ledger, client, attempt_id, cfg, auto=) 与 mock.patch 路径，
    返回旧字段（status_after/deliverables/missing/unverified 等）并携带
    新协议字段（result_version/notification_ids/collection_state/stale）。
    """
    info = _collection.collect(
        attempt_id, ledger=ledger, client=client, cfg=cfg, auto=auto,
        partial=partial, carry_manifest=carry_manifest,
        io=_CollectionIO, clock=clock or now_ms)
    stored = int(cfg.get("result_store_limit", 200000) or 200000)
    text = info.get("text_chars") or 0
    return {
        # ---- 旧字段（兼容既有调用/断言） ----
        "attempt": ledger.get_attempt(attempt_id),
        "result_path": info.get("result_path"),
        "diagnostic_path": info.get("diagnostic_path"),
        "text_chars": text,
        "text_truncated": text > stored and not info.get("partial"),
        "assistant_message_count": info.get("assistant_message_count", 0),
        "deliverables": info.get("deliverables") or [],
        "missing": info.get("missing") or [],
        "unverified": info.get("unverified") or [],
        "status_after": info.get("status"),
        "status": info.get("status"),
        # ---- 新协议字段 ----
        "partial": bool(info.get("partial")),
        "pending": bool(info.get("pending")),
        "effective": bool(info.get("effective")),
        "duplicate": bool(info.get("duplicate")),
        "result_version": info.get("result_version"),
        "notification_ids": info.get("notification_ids") or [],
        "classification": info.get("classification"),
        "collection_state": info.get("collection_state"),
        "ready_for_verification": info.get("ready_for_verification"),
        "next_collect_at": info.get("next_collect_at"),
        "stale": info.get("stale") or [],
        "coverage": info.get("coverage") or {},
        "collected_at": info.get("collected_at"),
    }


def run_checks(ledger, attempt_id: str, *, timeout: int = 120) -> list[dict]:
    """由协调者显式调用：执行 command 类型的验收命令并记录输出。

    检查证据绑定结果输入（D10）：evidence 记录执行时的结果版本与
    content_fingerprint；结果重新收集（新指纹）后旧检查证据在验收准备/
    提交侧失效为未验证，不能拿旧输入的检查判新输入。
    """
    att = ledger.get_attempt(attempt_id)
    result_ref = (att.get("metadata") or {}).get("result") or {}
    bound_fp = result_ref.get("content_fingerprint")
    bound_version = result_ref.get("version")
    out = []
    for d in ledger.deliverables(attempt_id):
        if d["kind"] != "command":
            continue
        started = time.time()
        try:
            proc = subprocess.run(
                d["target"], shell=True, cwd=att["directory"], text=True,
                capture_output=True, timeout=timeout,
            )
            evidence = {
                "command": d["target"],
                "exit": proc.returncode,
                "stdout": redact(proc.stdout, 4000),
                "stderr": redact(proc.stderr, 4000),
                "duration_ms": int((time.time() - started) * 1000),
                "timed_out": False,
                "content_fingerprint": bound_fp,
                "result_version": bound_version,
            }
            status = "present" if proc.returncode == 0 else "failed"
        except subprocess.TimeoutExpired as e:
            evidence = {
                "command": d["target"],
                "exit": None,
                "stdout": redact((e.stdout or "") if isinstance(e.stdout, str) else "", 4000),
                "stderr": redact((e.stderr or "") if isinstance(e.stderr, str) else "", 4000),
                "duration_ms": int((time.time() - started) * 1000),
                "timed_out": True,
                "content_fingerprint": bound_fp,
                "result_version": bound_version,
            }
            status = "failed"
        except OSError as e:
            evidence = {"command": d["target"], "exit": None,
                        "error": repr(e), "timed_out": False,
                        "content_fingerprint": bound_fp,
                        "result_version": bound_version}
            status = "failed"
        ledger.update_deliverable(d["id"], status=status, evidence=evidence)
        out.append({"target": d["target"], "status": status, "evidence": evidence})
    return out


def _check_declared_changed(ledger, att: dict, result_snapshot: dict) -> list[str]:
    """事务外复核声明产物（T35/F5）：收集后缺失/不可读/变化 → 目标列表。

    统一扫描契约（与基线/collect 同一套）：file 走 ns 级稳定哈希；
    dir/glob 走完整清单摘要并先判存在性——目录整体删除（含空目录，
    其摘要为 hash([])，单比较摘要会漏掉）必须先按 present 消失判变化；
    glob 清空后清单摘要由非空变 hash([]) 自然不等。必需条目（file/dir/glob）
    的收集版本快照摘要缺失也不能静默跳过：不可判定即视为变化，交由
    重新收集补证。预算取运行配置（load_config(ledger.home)）。
    carried 文件在收集后被改动亦在此暴露（D6/T21）；--run-checks 之后
    再次复核，检查命令改写产物同样被拦截。空列表表示与收集版本一致。
    """
    changed: list[str] = []
    artifacts = result_snapshot.get("artifact_deliverables") or []
    budget = _scan_budget_state(load_config(ledger.home))
    for d in artifacts:
        kind = d.get("kind")
        if kind not in ("file", "dir", "glob"):
            continue
        target = d["target"]
        if not os.path.isabs(target):
            target = os.path.join(att["directory"], target)
        target = os.path.expanduser(target)
        recorded = d.get("sha256") or d.get("manifest_sha256")
        if not recorded:
            # 收集版本快照缺少可比较摘要：不可判定不能静默放行（F5）
            changed.append(d["target"])
            continue
        if kind == "file":
            digest, _why = _hash_file_strict(target, budget)
            if digest != recorded:
                changed.append(d["target"])
        else:
            if kind == "dir":
                if not os.path.isdir(target):
                    # 目录整体消失（含收集时为空目录：hash([]) 相等会漏）
                    changed.append(d["target"])
                    continue
            manifest, complete, _reasons = _scan_artifact_manifest(
                target, kind, budget)
            computed = _manifest_digest(manifest) if complete else None
            if not complete or computed != recorded:
                changed.append(d["target"])
    return changed


# prepare 兼容别名（旧 results.py 内部用 _RUN_CHECKS 遮蔽 run_checks 参数）。
_RUN_CHECKS = run_checks


def verification_gate(ledger, attempt_id: str) -> dict:
    """验收门：交付物缺失/命令未验证时不允许自动 accepted。"""
    att = ledger.get_attempt(attempt_id)
    if att["status"] not in VERIFIABLE_STATES:
        raise LedgerError(
            "NOT_READY",
            f"尝试当前状态 {att['status']} 不能验收；需先完成结果收集",
        )
    return verification_gate_rows(ledger.deliverables(attempt_id))


def read_result_artifact(att: dict, task: dict) -> tuple[dict | None, str | None]:
    """读取并核验 collect_attempt 产出的结果 JSON（schema/attempt/task/generation）。

    只读真实结果文件；不把 result_path 非空当作“结果存在”。返回 (snapshot, error)：
    snapshot 记录内容 sha256、采集时点与关联字段；error 为稳定失败原因或 None。
    核验含存储正文完整性（D8）：identity 字段齐全但正文被改动的快照
    返回 text-digest-mismatch/text-integrity-invalid，不能过普通 verify。
    """
    rp = att.get("result_path")
    if not rp:
        return None, "missing-file"
    try:
        with open(os.path.expanduser(rp), "rb") as f:
            raw = f.read()
            st = os.fstat(f.fileno())
        data = json_loads(raw.decode("utf-8"), None)
    except (OSError, UnicodeDecodeError, ValueError) as e:
        return None, repr(e)[:300]
    if not isinstance(data, dict):
        return None, "not-json-object"
    if data.get("schema") != RESULT_ARTIFACT_SCHEMA:
        return None, "schema-mismatch"
    if data.get("attempt_id") != att["id"]:
        return None, "attempt-id-mismatch"
    if data.get("task_id") != task["id"]:
        return None, "task-id-mismatch"
    if data.get("generation") != att["generation"]:
        return None, "generation-mismatch"
    if data.get("session_id") != att["session_id"]:
        return None, "session-id-mismatch"
    if type(data.get("collected_at")) is not int or data["collected_at"] <= 0:
        return None, "collection-time-missing"
    # 存储正文完整性（D8，M2 分界）：新协议（protocol_version=rv1）快照必须
    # 严格验——非截断时 text 的 sha256 == text_full_sha256 且 text_chars==len；
    # 截断时 text_chars 为完整正文长度（>= len(text)），存储前缀由
    # stored_text_sha256 == sha256(text) 绑定（重算比对，换前缀即拒）。
    # legacy v1（无 protocol_version/完整性字段）按既有 schema/identity 读取，
    # 不强制新 digest（M2：旧代码生成的历史文件不可读侧修补伪摘要凑绿；
    # 新协议验收/混用由 B 的 RESULT_CHANGED/来源守卫在提交侧拒绝）。
    if data.get("protocol_version") == _result_rules.PROTOCOL_VERSION:
        text = data.get("text")
        full_sha = data.get("text_full_sha256")
        text_chars = data.get("text_chars")
        if not isinstance(text, str) or not isinstance(text_chars, int) \
                or text_chars < len(text) or not isinstance(full_sha, str) \
                or len(full_sha) != 64 \
                or any(c not in "0123456789abcdef" for c in full_sha):
            return None, "text-integrity-invalid"
        if not data.get("text_truncated"):
            if _verify_text_digest(text) != full_sha or text_chars != len(text):
                return None, "text-digest-mismatch"
        else:
            bound = data.get("stored_text_sha256")
            if not isinstance(bound, str) or bound != _verify_text_digest(text):
                return None, "text-integrity-invalid"
    return {
        "path": rp,
        "exists": True,
        "size": len(raw),
        "mtime": int(st.st_mtime * 1000),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "collected_at": att.get("result_collected_at"),
        "artifact_collected_at": data["collected_at"],
        "result_version": data.get("result_version"),
        "content_fingerprint": data.get("content_fingerprint"),
        "protocol_version": data.get("protocol_version"),
        "artifact_deliverables": data.get("deliverables"),
        "schema": data["schema"],
        "attempt_id": data["attempt_id"],
        "task_id": data["task_id"],
        "generation": data["generation"],
        "session_id": data["session_id"],
        "directory": att["directory"],
        "checked_at": now_ms(),
    }, None


# 旧私有名兼容别名。
_read_result_artifact = read_result_artifact


# ---- 验收兼容入口（委托 application，注入窄 I/O；签名与旧 oc_orch.results 一致） ----
def prepare_verification(ledger, attempt_id: str, *, result: str,
                         reason: str = "", evidence: Iterable[str] = (),
                         notes: str = "", force: bool = False,
                         run_checks: bool = False, check_timeout: int = 120,
                         actor: str = "coordinator") -> dict:
    """兼容入口：事务外验收证据准备，业务判定在 application.verification。"""
    return _verify.prepare_verification(
        attempt_id, result=result, reason=reason, evidence=evidence, notes=notes,
        force=force, run_checks=run_checks, check_timeout=check_timeout, actor=actor,
        ledger=ledger, read_result=read_result_artifact,
        run_checks_io=lambda aid, to: _RUN_CHECKS(ledger, aid, timeout=to),
        clock=now_ms,
        changed_check=lambda att, rs: _check_declared_changed(ledger, att, rs),
    )


def _bind_legacy_prepare(ledger):
    """绑定旧签名 prepare_verification；模块级函数名在调用时解析，保持 mock.patch
    oc_orch.results.prepare_verification 对 verify_attempt 编排的拦截能力。"""
    def prepare(attempt_id: str, *, result: str, reason: str = "",
                evidence: Iterable[str] = (), notes: str = "", force: bool = False,
                run_checks: bool = False, check_timeout: int = 120,
                actor: str = "coordinator") -> dict:
        return prepare_verification(ledger, attempt_id, result=result, reason=reason,
                                    evidence=evidence, notes=notes, force=force,
                                    run_checks=run_checks, check_timeout=check_timeout,
                                    actor=actor)
    return prepare


def verify_attempt(ledger, attempt_id: str, *, result: str,
                   reason: str = "", evidence: Iterable[str] = (),
                   notes: str = "", force: bool = False,
                   run_checks: bool = False, check_timeout: int = 120,
                   actor: str = "coordinator") -> dict:
    """兼容入口：统一验收用例编排，业务逻辑在 application.verification。"""
    return _verify.verify_attempt(
        attempt_id, result=result, reason=reason, evidence=evidence, notes=notes,
        force=force, run_checks=run_checks, check_timeout=check_timeout, actor=actor,
        ledger=ledger, read_result=read_result_artifact,
        run_checks_io=lambda aid, to: _RUN_CHECKS(ledger, aid, timeout=to),
        clock=now_ms,
        prepare=_bind_legacy_prepare(ledger),
    )

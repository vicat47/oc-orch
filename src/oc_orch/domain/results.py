"""纯结果规则：交付来源分类、有效收集门禁与稳定内容指纹（D5/D7/D8）。

零 I/O；输入为纯值（基线快照、当前扫描、carry 声明、消息摘要）。
adapters/results 提供 I/O，application/collection 编排，本模块只做判定。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

RESULT_SCHEMA = "oc-orch.result.v1"
BASELINE_SCHEMA = "oc-orch.baseline.v1"
CARRY_SCHEMA = "oc-orch.carry.v1"
PROTOCOL_VERSION = "rv1"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


_sha256_text = sha256_text  # 兼容内部旧名


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def provenance_for(item_baseline: dict | None, current: dict,
                   carried_decl: dict | None) -> dict:
    """单个交付目标的来源分类（D5 表）。

    item_baseline：基线中的目标记录（None=该项基线缺失）；
    current：{'present': bool, 'sha256': str|None, 'manifest_sha256': str|None}
    carried_decl：有效 carry 声明（{'baseline_sha256': ...}）或 None。
    返回 {'provenance': new|changed|unchanged|carried|unknown, 'detail': {...}}。
    """
    if not current.get("present"):
        return {"provenance": "unknown", "detail": {"reason": "current-scan-missing"}}
    cur_hash = current.get("sha256") or current.get("manifest_sha256")
    if cur_hash is None:
        return {"provenance": "unknown",
                "detail": {"reason": "current-scan-incomplete"}}
    if item_baseline is None:
        # 基线未覆盖该目标（从未声明或扫描缺失）：只有全新目标才算 new
        if current.get("declared_new_target"):
            return {"provenance": "new",
                    "detail": {"reason": "baseline-item-absent"}}
        return {"provenance": "unknown",
                "detail": {"reason": "baseline-item-missing"}}
    if item_baseline.get("status") in ("missing", "unknown"):
        # 基线时不存在/不可判定，现在存在：对文件即新产物
        if item_baseline.get("kind") == "file" and item_baseline.get("status") == "missing":
            return {"provenance": "new", "detail": {"reason": "baseline-missing-now-present"}}
        # dir/glob 的"基线时物理不存在"（status=missing）与"没扫出来"
        # （status=unknown，预算/权限/不稳定）必须区分：前者现在出现即 new
        # （物理事实可证明），后者即使 manifest 为空也只能 unknown——
        # 未知基线的空清单不等于物理不存在（协调者反例 3）。
        if item_baseline.get("status") == "missing":
            return {"provenance": "new",
                    "detail": {"reason": "baseline-missing-now-present"}}
        return {"provenance": "unknown",
                "detail": {"reason": f"baseline-{item_baseline.get('status')}"}}
    base_hash = (item_baseline.get("sha256")
                 or item_baseline.get("manifest_sha256"))
    if base_hash is None:
        return {"provenance": "unknown",
                "detail": {"reason": "baseline-hash-missing"}}
    if carried_decl is not None:
        if carried_decl.get("baseline_sha256") == base_hash == cur_hash:
            return {"provenance": "carried",
                    "detail": {"reason": "carry-verified",
                               "source_attempt_id": carried_decl.get(
                                   "source_attempt_id")}}
        return {"provenance": "unchanged" if base_hash == cur_hash else "changed",
                "detail": {"reason": "carry-hash-mismatch"}}
    if base_hash == cur_hash:
        return {"provenance": "unchanged", "detail": {"reason": "hash-equal"}}
    return {"provenance": "changed", "detail": {"reason": "hash-different"}}


def validate_carry_items(manifest: Any, *, attempt_id: str,
                         declared_targets: set[str],
                         source_attempts: dict[str, dict],
                         current_task_id: str | None = None,
                         current_generation: int | None = None,
                         ) -> tuple[list[dict], list[dict]]:
    """校验 carry 清单（D6）。返回 (有效声明列表, 输入错误列表)。

    - 精确绑定当前 attempt 与已声明目标；无通配"全部沿用"；
    - 重复目标、未知目标、空理由、无效摘要为输入错误；
    - source_attempt_id 指定时必须是同任务更早代次且证据摘要相符。
      current_task_id/current_generation 为调用方提供的防御上下文：提供时
      在本层再校验一次"同任务更早代次"，不依赖上游过滤的正确性；未提供时
      沿用上游过滤结果（保持既有调用兼容）。
    """
    errors: list[dict] = []
    valid: list[dict] = []
    if not isinstance(manifest, dict):
        return [], [{"code": "CARRY_MANIFEST_INVALID", "detail": "清单必须是对象"}]
    if manifest.get("schema") != CARRY_SCHEMA:
        return [], [{"code": "CARRY_SCHEMA_MISMATCH",
                     "detail": f"schema 应为 {CARRY_SCHEMA}"}]
    if manifest.get("attempt_id") != attempt_id:
        return [], [{"code": "CARRY_ATTEMPT_MISMATCH",
                     "detail": f"清单绑定 {manifest.get('attempt_id')!r}，"
                               f"当前尝试 {attempt_id}"}]
    items = manifest.get("items")
    if not isinstance(items, list) or not items:
        return [], [{"code": "CARRY_ITEMS_EMPTY", "detail": "缺少沿用声明"}]
    seen: set[str] = set()
    for raw in items:
        if not isinstance(raw, dict):
            errors.append({"code": "CARRY_ITEM_INVALID", "detail": raw})
            continue
        target = raw.get("target")
        base_hash = raw.get("baseline_sha256")
        reason = (raw.get("reason") or "").strip()
        if target in seen:
            errors.append({"code": "CARRY_TARGET_DUPLICATE", "detail": str(target)})
            continue
        seen.add(target)
        if target not in declared_targets:
            errors.append({"code": "CARRY_TARGET_UNDECLARED", "detail": str(target)})
            continue
        if not reason:
            errors.append({"code": "CARRY_REASON_REQUIRED", "detail": str(target)})
            continue
        if not isinstance(base_hash, str) or len(base_hash) != 64 \
                or any(c not in "0123456789abcdef" for c in base_hash):
            errors.append({"code": "CARRY_BASELINE_SHA_INVALID", "detail": str(target)})
            continue
        source_id = raw.get("source_attempt_id")
        if source_id is not None:
            src = source_attempts.get(source_id)
            if src is None:
                errors.append({"code": "CARRY_SOURCE_UNKNOWN",
                               "detail": f"{target} -> {source_id}"})
                continue
            if src.get("generation") is None or src.get("task_id") is None:
                errors.append({"code": "CARRY_SOURCE_UNKNOWN",
                               "detail": f"{target} -> {source_id}"})
                continue
            if (current_task_id is not None
                    and src.get("task_id") != current_task_id):
                errors.append({"code": "CARRY_SOURCE_WRONG_TASK",
                               "detail": f"{target} -> {source_id}"})
                continue
            if (current_generation is not None
                    and src.get("generation") is not None
                    and src.get("generation") >= current_generation):
                errors.append({"code": "CARRY_SOURCE_NOT_EARLIER",
                               "detail": f"{target} -> {source_id}"})
                continue
        valid.append({"target": target, "baseline_sha256": base_hash,
                      "source_attempt_id": source_id,
                      "reason": reason,
                      "reason_sha256": _sha256_text(reason)})
    return valid, errors


def effective_collection(*, assistant_text_sha: str | None,
                         assistant_count: int,
                         deliverables: list[dict]) -> bool:
    """有效收集门禁（D7）：成功执行 + 本轮实质证据。

    实质证据 = 非空 assistant 实质回报 或 ≥1 个 new/changed 声明交付物；
    carry 与 unchanged 都不算新回报。
    """
    if assistant_text_sha:
        return True
    return any(d.get("provenance") in ("new", "changed") for d in deliverables)


def stale_deliverables(deliverables: list[dict]) -> list[dict]:
    return [d for d in deliverables
            if d.get("required") and d.get("provenance") in ("unchanged", "unknown")
            and d.get("kind") in ("file", "dir", "glob")]


def stale_provenance_rows(rows: list[dict] | None) -> dict[str, list[str]]:
    """已收集 provenance 摘要的来源门输入（F4）：返回 stale/unknown 目标清单。

    required 的 file/dir/glob 项：未声明沿用（unchanged）与不可判定（unknown）
    都不能经普通 verify；carried/new/changed 满足来源门。行集缺失（legacy
    旧台账无 deliverable_provenance 摘要）不由本函数阻断，是否兼容由调用方
    按协议版本显式决定，不用"缺行"冒充"来源已证明"。
    """
    stale: list[str] = []
    unknown: list[str] = []
    for p in rows or []:
        if not p.get("required") or p.get("kind") not in ("file", "dir", "glob"):
            continue
        if p.get("provenance") == "unchanged":
            stale.append(p.get("target"))
        elif p.get("provenance") == "unknown":
            unknown.append(p.get("target"))
    return {"stale": stale, "unknown": unknown}


def command_checks_bound(deliverable_rows: list[dict] | None,
                         content_fingerprint: str | None) -> list[str]:
    """command 检查证据必须绑定结果输入指纹（D10）：返回不可判定的目标清单。

    不可用旧输入的检查判新输入：新协议（当前结果已带 content_fingerprint）
    下，检查证据须绑定同一指纹；不匹配（旧输入的检查）或证据不可解析均视为
    未验证。双方都无指纹（legacy/未收集）时不在此失效，由 M2 兼容路径处理。
    """
    if not content_fingerprint:
        return []
    out: list[str] = []
    for d in deliverable_rows or []:
        if d.get("kind") != "command":
            continue
        ev = d.get("evidence")
        if isinstance(ev, str):
            try:
                ev = json.loads(ev)
            except ValueError:
                ev = None
        if not isinstance(ev, dict) \
                or ev.get("content_fingerprint") != content_fingerprint:
            out.append(d.get("target"))
    return out


def content_fingerprint(*, attempt: dict, execution: dict,
                        message_ids: list[str], text_sha256: str,
                        deliverables: list[dict]) -> str:
    """稳定内容指纹（D8）：剔除采集时间/mtime/观察来源等易变字段。"""
    payload = {
        "protocol_version": PROTOCOL_VERSION,
        "attempt_id": attempt["id"], "task_id": attempt["task_id"],
        "session_id": attempt.get("session_id"),
        "generation": attempt.get("generation"),
        "input_message_id": attempt.get("dispatch_msg_id"),
        "execution": {
            "outcome": execution.get("outcome"),
            "end_at": execution.get("end_at"),
        },
        "messages": {"ids": sorted(message_ids), "text_sha256": text_sha256,
                     "count": len(message_ids)},
        "deliverables": [
            {"target": d["target"], "kind": d["kind"],
             "sha256": d.get("sha256") or d.get("manifest_sha256"),
             # 观察基线精化（unknown→unchanged）不改变内容身份：只区分
             # "本轮新证据"（new/changed）与"非新"（stable 标记）。
             "new_evidence": d.get("provenance") in ("new", "changed")}
            for d in sorted(deliverables, key=lambda x: x["target"])
        ],
        "carry": sorted(
            [{"target": c["target"], "baseline_sha256": c["baseline_sha256"],
              "source": c.get("source_attempt_id"),
              "reason_sha256": c.get("reason_sha256")}
             for c in (attempt.get("carry_records") or [])],
            key=lambda x: x["target"]),
    }
    return _sha256_text(canonical_json(payload))


def result_version(*, content_fp: str, missing: list[str], stale: list[str],
                   unverified: list[str],
                   checks: dict[str, dict] | None = None) -> str:
    """结果版本 = 内容指纹 + 可观察交付门状态（剔除时间类字段）。"""
    gate = {
        "missing": sorted(missing), "stale": sorted(stale),
        "unverified": sorted(unverified),
        "checks": {t: {"status": c.get("status"), "exit": c.get("exit")}
                   for t, c in sorted((checks or {}).items())},
    }
    return f"{PROTOCOL_VERSION}-{_sha256_text(canonical_json({'content_fingerprint': content_fp, 'gate': gate}))}"


CLASSIFICATION_TABLE = (
    ("RESULT_MISSING", "result_missing"),
    ("COLLECT_FAILED", "result_collect_failed"),
    ("DELIVERABLE_MISSING", "result_ready"),
    ("DELIVERABLE_STALE", "result_ready"),
    ("CHECKS_FAILED", "result_ready"),
    ("CHECKS_PENDING", "result_ready"),
    ("RESULT_READY", "result_ready"),
)


def classify_publication(*, effective: bool, missing: list[str], stale: list[str],
                         unverified: list[str], checks_failed: bool,
                         checks_pending: bool) -> tuple[str, str]:
    """分类优先级（D9 表）：返回 (classification, kind)。"""
    if not effective:
        return "RESULT_MISSING", "result_missing"
    if missing:
        return "DELIVERABLE_MISSING", "result_ready"
    if stale:
        return "DELIVERABLE_STALE", "result_ready"
    if checks_failed:
        return "CHECKS_FAILED", "result_ready"
    if checks_pending or unverified:
        return "CHECKS_PENDING", "result_ready"
    return "RESULT_READY", "result_ready"


def ready_for_verification(*, missing: list[str], stale: list[str],
                           unverified: list[str]) -> bool:
    return not (missing or stale or unverified)
